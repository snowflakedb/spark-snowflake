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

  def tryBuild() : Option[QueryBuilder] = Try(build).toOption

  private def build() : QueryBuilder = {
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
    val childPlans : Seq[LogicalPlan] = plan.children

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

    // Advance alias label
    subqueryAlias.next

    plan match {
      case Filter(condition, _) =>
        PartialQuery(
          alias = alias,
          output = subTree.output,
          inner = subTree,
          suffix = Some(SQLBuilder
            .withFields(subTree.qualifiedOutput)
            .raw(" WHERE ").addExpression(condition)
          )
        )

      case Project(fields, child) =>
        for {
          subTree <- buildQueryTree(fieldIdIter, alias.child, child)
          expressions = renameExpressions(fieldIdIter, fields)
        } yield PartialQuery(
          alias = alias,
          output = expressions.map(_.toAttribute),
          prefix = SQLBuilder
            .withFields(subTree.qualifiedOutput)
            .maybeAddExpressions(expressions, ", "),
          inner = subTree
        )

      // NOTE: The Catalyst optimizer will sometimes produce an aggregate with empty fields.
      // (Try "select count(*) from (select count(*) from foo) bar".) Spark seems to treat
      // it as a single empty tuple; we're not sure whether this is defined behavior, so we
      // let Spark handle that case to avoid any inconsistency.
      case Aggregate(groups, fields, child) =>
        for {
          subTree <- buildQueryTree(fieldIdIter, alias.child, child)
          expressions = renameExpressions(fieldIdIter, fields)
        } yield {
          PartialQuery(
            alias = alias,
            output = expressions.map(_.toAttribute),
            prefix = {
              val sb = SQLBuilder.withFields(subTree.qualifiedOutput)
              if (expressions.isEmpty) {
                Some(sb.raw("COUNT(*)"))
              }
              else {
                Some(sb.addExpressions(expressions, ", "))
              }
            },
            inner = subTree,
            suffix = SQLBuilder
              .withFields(subTree.qualifiedOutput)
              .raw(" GROUP BY ").maybeAddExpressions(groups, ", ")
          )
        }

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
        } yield buildSortLimit(Literal(Long.MaxValue), orderExpr, alias, subTree)

      case Join(left, right, Inner, condition) => {
        val (leftAlias, rightAlias) = alias.fork
        for {
          leftSubTree <- buildQueryTree(fieldIdIter, leftAlias.child, left)
          rightSubTree <- buildQueryTree(fieldIdIter, rightAlias.child, right)
          if leftSubTree.sharesCluster(rightSubTree)
          qualifiedOutput = leftSubTree.qualifiedOutput ++ rightSubTree.qualifiedOutput
          renamedQualifiedOutput = renameExpressions(fieldIdIter, qualifiedOutput)
        } yield {
          JoinQuery(
            alias = alias,
            output = renamedQualifiedOutput.map(_.toAttribute),
            projection = SQLBuilder
              .withFields(qualifiedOutput)
              .addExpressions(renamedQualifiedOutput, ", "),
            condition = condition.map { c =>
              SQLBuilder
                .withFields(qualifiedOutput)
                .addExpression(c)
            },
            left = leftSubTree,
            right = rightSubTree
          )
        }
      }

      case l@UnpackLogicalRelation(r: SnowflakeRelation) => Some(BaseQuery(alias, r, l.output))

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

  /**
    * Assign field names to each expression for a given alias
    */
  def renameExpressions(fieldIdIter: Iterator[String], expressions: Seq[NamedExpression]): Seq[NamedExpression] =
  expressions.map {
    // We need to special case Alias, since this is not valid SQL:
    // select (foo as bar) as baz from ...
    case a@Alias(child: Expression, name: String) => {
      val metadata = MetadataUtils.preserveOriginalName(a)
      Alias(child, fieldIdIter.next)(a.exprId, None, Some(metadata))
    }
    case expr: NamedExpression => {
      val metadata = MetadataUtils.preserveOriginalName(expr)
      Alias(expr, fieldIdIter.next)(expr.exprId, None, Some(metadata))
    }
  }

  private def exprToNames(exprs: Seq[Expression]) : Seq[String] = {
    exprs.map { e => e.toString }
  }
}

object MetadataUtils {
  val METADATA_ORIGINAL_NAME = "originalColumnName"

  def preserveOriginalName(expr: NamedExpression): Metadata = {
    val meta = expr.metadata
    if (!meta.contains(METADATA_ORIGINAL_NAME)) {
      new MetadataBuilder()
        .withMetadata(meta)
        .putString(METADATA_ORIGINAL_NAME, expr.name)
        .build
    } else {
      meta
    }
  }

  def getOriginalName(expr: NamedExpression): Option[String] = {
    if (expr.metadata.contains(METADATA_ORIGINAL_NAME)) {
      Some(expr.metadata.getString(METADATA_ORIGINAL_NAME))
    } else {
      None
    }
  }

}
