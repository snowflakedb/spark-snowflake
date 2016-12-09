package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{SnowflakeRelation, UnpackLogicalRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

import scala.util.Try

/**
  * Created by ema on 11/30/16.
  */

class QueryBuilder(plan: LogicalPlan) {

  // Iterators are not thread safe
  lazy val columnAlias = Iterator.from(1).map(n => s"column_$n")
  lazy val subqueryAlias = Iterator.from(0).map(n => s"subquery_$n")
  lazy val source = getBase()

  var treeRoot: SnowflakeQuery = new DummyQuery

  def tryBuild(): Option[QueryBuilder] = Try(build).toOption

  private def build(): QueryBuilder = {
    treeRoot = generateQueries(plan).get
    this
  }

  // Returns the base (source table) query for this entire query tree.
  // We use the source node's relation object's buildScan() to issue
  // the full query to Snowflake and dump to S3.
  def getBase(): SourceQuery = {
    treeRoot.find {
      case q: SourceQuery => q
    }.getOrElse(
      throw new SnowflakePushdownException("Uh Oh!")
    )
  }

  private def generateQueries(plan: LogicalPlan): Option[SnowflakeQuery] = {

    // Fetch all children (may contain two if a join)
    val childPlans: Seq[LogicalPlan] = plan.children

    // Must be a base query
    if (childPlans == Nil) {
      return plan match {
        case LogicalRelation(sfRelation: SnowflakeRelation, _, _) =>
          Some(SourceQuery(sfRelation, subqueryAlias.next))
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
        Some(FilterQuery(
          condition,
          subQuery,
          subqueryAlias.next
        ))

      case Project(fields, _) =>
        Some(ProjectQuery(fields,
          columnAlias,
          subQuery,
          subqueryAlias.next))

      // NOTE: The Catalyst optimizer will sometimes produce an aggregate with empty fields.
      // (Try "select count(*) from (select count(*) from foo) bar".) Spark seems to treat
      // it as a single empty tuple; we're not sure whether this is defined behavior, so we
      // let Spark handle that case to avoid any inconsistency.
      case Aggregate(groups, fields, _) =>
        Some(AggregateQuery(
          fields,
          groups,
          columnAlias,
          subQuery,
          subqueryAlias.next))

      /*
      case Limit(limitExpr, child) =>
        for {
          subTree <- buildQueryTree(fieldIdIter, alias.child, child)
        } yield {
          buildSortLimit(limitExpr, Seq(), alias, subTree)
        }

      case Limit(limitExpr, Sort(orderExpr, /* global= */ true, child)) =>
        for {
          subTree <- buildQueryTree(fieldIdIter, alias.child, child)
        } yield buildSortLimit(limitExpr, orderExpr, alias, subTree)

      case Sort(orderExpr, /* global= */ true, Limit(limitExpr, child)) =>
        for {
          subTree <- buildQueryTree(fieldIdIter, alias.child, child)
        } yield buildSortLimit(limitExpr, orderExpr, alias, subTree)

      case Sort(orderExpr, /* global= */ true, child) =>
        for {
          subTree <- buildQueryTree(fieldIdIter, alias.child, child)
        } yield buildSortLimit(Literal(Long.MaxValue), orderExpr, alias, subTree) */

      case Join(_, _, Inner, condition) => {
        if (!subQuery.sharesCluster(optQuery.get)) None
        else
          Some(JoinQuery(
            columnAlias,
            subQuery,
            optQuery.get,
            condition,
            subqueryAlias.next
          ))
      }

      case _ => None
    }

  }

  def buildSortLimit(limitExpr: Expression, orderExpr: Seq[Expression], alias: QueryAlias, subTree: AbstractQuery): AbstractQuery =
    PartialQuery(
      alias = alias,
      output = subTree.output,
      inner = subTree,
      suffix = Some({
        val sb = SQLBuilder.withFields(subTree.qualifiedOutput)
        if (orderExpr.nonEmpty) {
          sb.raw(" ORDER BY ").addExpressions(orderExpr, ", ")
        }
        sb.raw(" LIMIT ").addExpression(limitExpr)
      })
    )
}

