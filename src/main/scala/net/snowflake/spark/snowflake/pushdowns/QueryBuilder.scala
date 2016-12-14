package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{SnowflakePushdownException, SnowflakeRelation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
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
private[snowflake] class QueryBuilder(plan: LogicalPlan) {

  lazy val subqueryAlias = Iterator.from(0).map(n => s"subquery_$n")

  lazy val physicalRDD = toRDD[InternalRow]

  lazy val tryBuild: Option[QueryBuilder] = if (treeRoot == null) None else Some(this)

  lazy val query: String = {
    checkTree()
    val query = treeRoot.getQuery()
    val pretty = treeRoot.getPrettyQuery()
    SnowflakeQuery.log.info(s"""Generated query: '$pretty'""")
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
        null
      }
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

    // Fetch all children (may contain two if a join)
    val childPlans: Seq[LogicalPlan] = plan.children

    // Must be a base query
    if (childPlans == Nil) {
      return plan match {
        case l @ LogicalRelation(sfRelation: SnowflakeRelation, _, _) =>
          Some(SourceQuery(sfRelation, l.output, subqueryAlias.next))
        case _ => None
      }
    }

    val subQuery = {
      val s = generateQueries(childPlans.head)
      if (s.isEmpty) return None
      else
        s.get
    }

    // Contains query if binary node, None if not
    val optQuery = childPlans.lift(1) match {
      case Some(child) => generateQueries(child)
      case None => None
    }

    plan match {
      case Filter(condition, _) =>
        Some(
          FilterQuery(
            condition,
            subQuery,
            subqueryAlias.next
          ))

      case Project(fields, _) =>
        Some(ProjectQuery(fields, subQuery, subqueryAlias.next))

      // NOTE: The Catalyst optimizer will sometimes produce an aggregate with empty fields.
      // (Try "select count(*) from (select count(*) from foo) bar".) Spark seems to treat
      // it as a single empty tuple; we're not sure whether this is defined behavior, so we
      // let Spark handle that case to avoid any inconsistency.
      case Aggregate(groups, fields, _) =>
        Some(AggregateQuery(fields, groups, subQuery, subqueryAlias.next))

      case Limit(limitExpr, child) =>
        Some(SortLimitQuery(limitExpr, Seq.empty, subQuery, subqueryAlias.next))

      case Limit(limitExpr, Sort(orderExpr, /* global= */ true, child)) =>
        Some(SortLimitQuery(limitExpr, orderExpr, subQuery, subqueryAlias.next))

      case Sort(orderExpr, /* global= */ true, Limit(limitExpr, child)) =>
        Some(SortLimitQuery(limitExpr, orderExpr, subQuery, subqueryAlias.next))

      case Sort(orderExpr, /* global= */ true, child) =>
        Some(SortLimitQuery(Literal(Long.MaxValue), orderExpr, subQuery, subqueryAlias.next))

      case Join(_, _, Inner, condition) => {
        if (!subQuery.sharesCluster(optQuery.get)) None
        else
          Some(
            JoinQuery(
              subQuery,
              optQuery.get,
              condition,
              subqueryAlias.next
            ))
      }

      case _ => None
    }
  }
}
