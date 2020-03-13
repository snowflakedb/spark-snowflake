package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
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
private[querygeneration] object StringStatement {

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
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case _: Ascii | _: Lower | _: Substring | _: StringLPad | _: StringRPad |
          _: StringTranslate | _: StringTrim | _: StringTrimLeft |
          _: StringReverse | _: StringTrimRight | _: Substring | _: Upper =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case Concat(children) =>
        val rightSide =
          if (children.length > 2) Concat(children.drop(1)) else children(1)
        ConstantString("CONCAT") + blockStatement(
          convertStatement(children.head, fields) + "," +
            convertStatement(rightSide, fields)
        )

      case Like(left, right) =>
        convertStatement(left, fields) + "LIKE" + convertStatement(
          right,
          fields
        )

      case _ => null
    })
  }
}
