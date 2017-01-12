package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
  * Extractor for aggregate-style expressions.
  */
private[QueryGeneration] object AggregationExpression {

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(expAttr: (Expression, Seq[Attribute])): Option[String] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    // Use this hack because AggregateExpression.isDistinct is not accessible from our package
    val distinct: String =
      if (expr.sql contains "(DISTINCT ") "DISTINCT " else ""
    // Take only the first child, as all of the functions below have only one.
    expr.children.headOption.flatMap(agg_fun => {
      Option(agg_fun match {
        case _: Average | _: Corr | _: CovPopulation | _: CovSample |
            _: Count | _: Max | _: Min | _: Sum | _: StddevPop |
            _: StddevSamp | _: VariancePop | _: VarianceSamp =>
          agg_fun.prettyName.toUpperCase + block(
            distinct + convertExpressions(fields, agg_fun.children: _*))

        case _ => null
      })
    })
  }
}
