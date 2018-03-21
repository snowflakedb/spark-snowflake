package net.snowflake.spark.snowflake.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Attribute, DateAdd, Expression, Month, Quarter, Year}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object DateExpression {

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

    Option(
      expr match {
        case DateAdd(startDate, days) =>
          "DATEADD(day, " + convertExpression(days, fields) + ", " +
            convertExpression(startDate, fields) + ")"

        case _: Month | _: Quarter | _: Year  =>
          expr.prettyName.toUpperCase + block(
            convertExpressions(fields, expr.children: _*))

        case _ => null
      }
    )
  }
}
