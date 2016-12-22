package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Asin, Atan, Attribute, Ceil, CheckOverflow, Cos, Cosh, Expression, Floor, Greatest, Least, Log, Pi, Pmod, Rand, Round, Sin, Sinh, Sqrt, Tan, Tanh}

/**
  * Extractor for boolean expressions (return true or false).
  */
private[QueryGeneration] object NumericExpression {

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
        case Abs(_) | Acos(_) | Cos(_) | Tan(_) | Tanh(_) | Cosh(_) | Atan(_) |
            Floor(_) | Sin(_) | Log(_) | Asin(_) | Sqrt(_) | Ceil(_) |
            Sqrt(_) | Sinh(_) =>
          expr.prettyName.toUpperCase + block(
            convertExpression(expr.children.head, fields))

        case CheckOverflow(child, _) => convertExpression(child, fields)

        case Greatest(children) =>
          "GREATEST" + block(convertExpressions(fields, children:_*))
        case Least(children) =>
          "LEAST" + block(convertExpressions(fields, children:_*))

        case Pi() => "PI()"

        case Rand(seed) => "RANDOM" + block(seed.toString)
        case Round(child, scale) =>
          "ROUND" + block(convertExpressions(fields, child, scale))

        case _ => null
      }
    )
  }
}
