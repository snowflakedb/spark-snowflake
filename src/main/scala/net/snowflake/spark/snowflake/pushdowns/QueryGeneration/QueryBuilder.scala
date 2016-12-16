package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import net.snowflake.spark.snowflake.{SnowflakePushdownException, SnowflakeRelation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.reflect.ClassTag

/**
  * Created by ema on 11/30/16.
  *
  * This class takes a Spark LogicalPlan and attempts to generate
  * a query for Snowflake using tryBuild. Here we use lazy instantiation
  * to avoid recomputation.
  *
  */
private class QueryBuilder(plan: LogicalPlan) {

  lazy val alias = Iterator.from(0).map(n => s"subquery_$n")

  lazy val rdd = toRDD[InternalRow]

  lazy val tryBuild: Option[QueryBuilder] = if (treeRoot == null) None else Some(this)

  lazy val query: String = {
    checkTree()
    val query = treeRoot.getPrettyQuery()
    SnowflakeQuery.log.info(s"""Generated query: '$query'""")
    query
  }
  // Fetch the output attributes.
  lazy val getOutput = {
    checkTree()
    treeRoot.output
  }
  // Finds the SourceQuery in this given tree.
  private lazy val source = {
    checkTree()
    treeRoot.find {
      case q: SourceQuery => q
    }.getOrElse(
      throw new SnowflakePushdownException(
        "Something went wrong: a query tree was generated with no " +
          "Snowflake SourceQuery found.")
    )
  }
  private lazy val treeRoot: SnowflakeQuery = {
    try {
      generateQueries(plan).get
    } catch {
      case e: MatchError => {
        SnowflakeQuery.log.debug("Could not generate a query.")
      }
      null
    }
  }

  def toRDD[T: ClassTag]: RDD[T] = {
    source.relation.buildScanFromSQL[T](query)
  }

  private def checkTree(): Unit = {
    if (treeRoot == null) {
      throw new SnowflakePushdownException("QueryBuilder's tree accessed without generation.")
    }
  }

  private def generateQueries(plan: LogicalPlan): Option[SnowflakeQuery] = {

    plan match {
      case l @ LogicalRelation(sfRelation: SnowflakeRelation, _, _) =>
        Some(SourceQuery(sfRelation, l.output, alias.next))

      case Unary(child) => {

        generateQueries(child) map { subQuery =>
          plan match {

            case Filter(condition, _)         => FilterQuery(condition, subQuery, alias.next)
            case Project(fields, _)           => ProjectQuery(fields, subQuery, alias.next)
            case Aggregate(groups, fields, _) => AggregateQuery(fields, groups, subQuery, alias.next)
            case Limit(limitExpr, _)          => SortLimitQuery(limitExpr, Seq.empty, subQuery, alias.next)
            case Limit(limitExpr, Sort(orderExpr, /* global= */ true, _)) =>
              SortLimitQuery(limitExpr, orderExpr, subQuery, alias.next)

            case Sort(orderExpr, /* global= */ true, Limit(limitExpr, _)) =>
              SortLimitQuery(limitExpr, orderExpr, subQuery, alias.next)
            case Sort(orderExpr, /* global= */ true, _) =>
              SortLimitQuery(Literal(Long.MaxValue), orderExpr, subQuery, alias.next)
          }
        }
      }

      case Binary(left, right) =>
        generateQueries(left).flatMap { l =>
          generateQueries(right) map { r =>
            plan match {
              case Join(_, _, Inner, condition) => JoinQuery(l, r, condition, alias.next)
            }
          }
        }

      case _ => None
    }
  }
}

private[snowflake] object QueryBuilder {
  def getRDDFromPlan(plan: LogicalPlan): Option[(Seq[Attribute], RDD[InternalRow])] = {
    val qb = new QueryBuilder(plan)

    qb.tryBuild.map { executedBuilder =>
      (executedBuilder.getOutput, executedBuilder.rdd)
    }

  }
}
