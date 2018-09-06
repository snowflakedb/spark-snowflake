package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, EmptySnowflakeSQLStatement, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
  * Extractor for aggregate-style expressions.
  */
private[querygeneration] object AggregationExpression {

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

    // Take only the first child, as all of the functions below have only one.
    expr.children.headOption.flatMap(agg_fun => {
      Option(agg_fun match {
        case _: Average | _: Corr | _: CovPopulation | _: CovSample |
            _: Count | _: Max | _: Min | _: Sum | _: StddevPop |
            _: StddevSamp | _: VariancePop | _: VarianceSamp => {
          val distinct: String =
            if (expr.sql contains "(DISTINCT ") "DISTINCT " else ""
          agg_fun.prettyName.toUpperCase + block(
            distinct + convertExpressions(fields, agg_fun.children: _*))
        }
        case _ => null
      })
    })
  }
}

private[querygeneration] object AggregationStatement {
  def unapply(expAttr: (Expression, Seq[Attribute])): Option[SnowflakeSQLStatement] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    // Take only the first child, as all of the functions below have only one.
    expr.children.headOption.flatMap(agg_fun => {
      Option(agg_fun match {
        case _: Average | _: Corr | _: CovPopulation | _: CovSample |
             _: Count | _: Max | _: Min | _: Sum | _: StddevPop |
             _: StddevSamp | _: VariancePop | _: VarianceSamp => {
          val distinct: SnowflakeSQLStatement =
            if (expr.sql contains "(DISTINCT ") ConstantString("DISTINCT") !
            else EmptySnowflakeSQLStatement()

          ConstantString(agg_fun.prettyName.toUpperCase) +
            blockStatement(
              distinct + convertStatements(fields, agg_fun.children: _*)
            )
        }
        case _ => null
      })
    })
  }
}
