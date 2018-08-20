package net.snowflake.spark.snowflake.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.expressions.{
  Ascii,
  Attribute,
  Concat,
  Expression,
  Like,
  Lower,
  StringLPad,
  StringRPad,
  StringReverse,
  StringTranslate,
  StringTrim,
  StringTrimLeft,
  StringTrimRight,
  Substring,
  Upper
}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object StringExpression {

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
        case _: Ascii | _: Lower | _: Substring | _: StringLPad |
            _: StringRPad | _: StringTranslate |
            _: StringTrim | _: StringTrimLeft | _: StringTrimRight |
            _: Substring | _: Upper =>
          expr.prettyName.toUpperCase + block(
            convertExpressions(fields, expr.children: _*))

        case Concat(children) =>
          val rightSide =
            if (children.length > 2) Concat(children.drop(1)) else children(1)
          "CONCAT" + block(
            convertExpression(children.head, fields) + ", " + convertExpression(
              rightSide,
              fields))

        case Like(left, right) =>
          convertExpression(left, fields) + " LIKE " + convertExpression(
            right,
            fields)

        case _ => null
      }
    )
  }
}
