package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  BinaryOperator,
  Expression,
  Literal
}

/**
  * Extractor for basic (attributes and literals) expressions.
  */
private[QueryGeneration] object BasicExpression {

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

    Option(expr match {
      case a: Attribute => addAttribute(a, fields)
      case b: BinaryOperator =>
        block(
          convertExpression(b.left, fields) + s"${b.symbol}" +
            convertExpression(b.right, fields)
        )
      case l: Literal => l.toString

      case _ => null
    })
  }
}
