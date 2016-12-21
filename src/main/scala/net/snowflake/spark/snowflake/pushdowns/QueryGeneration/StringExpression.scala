package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions.{Ascii, Attribute, Expression, Lower, StringLPad, StringRPad, StringReverse, StringTrim, Upper}

/**
  * Extractor for boolean expressions (return true or false).
  */
private[QueryGeneration] object StringExpression {

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
        case Ascii(_) | Lower(_) | StringReverse(_) | StringTrim(_) | Upper(_) =>
          expr.prettyName.toUpperCase + block(
            convertExpression(expr.children.head, fields))

        case StringLPad(str, len, pad) =>
          "LPAD" + block(convertExpressions(fields, str, len, pad))
        case StringRPad(str, len, pad) =>
          "RPAD" + block(convertExpressions(fields, str, len, pad))

        case _ => null
      }
    )
  }
}
