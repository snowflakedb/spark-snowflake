package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{SnowflakeRelation, UnpackLogicalRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.apache.spark.sql.{SparkSession, Strategy}

class SnowflakeStrategy(session: SparkSession) extends Strategy{
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    val fieldIdIter = Iterator.from(1).map(n => s"f_$n")

    // remove empty projects and subqueries
    val cleanedPlan = plan.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })

    try {
      val alias = QueryAlias("query")

      buildQueryTree(fieldIdIter, alias, cleanedPlan) match {
        case Some(queryTree) => Seq(SnowflakePlan.buildRDDFromQuery(session, queryTree))
        case _ => Nil
      }
    } catch {
      // In the case that we fail to handle the plan we will raise MatchError.
      // Return Nil to let another strategy handle this tree.
      case e: MatchError => {
        logDebug(s"Failed to match plan: $e")
        Nil
      }
    }
  }

  def buildQueryTree(fieldIdIter: Iterator[String], alias: QueryAlias, plan: LogicalPlan): Option[AbstractQuery] = plan match {
    case Filter(condition, child) =>
      for {
        subTree <- buildQueryTree(fieldIdIter, alias.child, child)
      } yield PartialQuery(
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