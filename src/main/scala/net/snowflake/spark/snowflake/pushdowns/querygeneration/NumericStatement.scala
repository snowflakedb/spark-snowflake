package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, LongVariable, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Asin, Atan, Attribute, Ceil, CheckOverflow, Cos, Cosh, Exp, Expression, Floor, Greatest, Least, Log, Pi, Pow, PromotePrecision, Rand, Round, Sin, Sinh, Sqrt, Tan, Tanh, UnaryMinus}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object NumericStatement {

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
  def unapply(expAttr: (Expression, Seq[Attribute])): Option[SnowflakeSQLStatement] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    Option(
      expr match {
        case _: Abs | _: Acos | _: Cos | _: Tan | _: Tanh | _: Cosh | _: Atan |
             _: Floor | _: Sin | _: Log | _: Asin | _: Sqrt | _: Ceil |
             _: Sqrt | _: Sinh | _: Greatest | _: Least | _: Exp =>
          ConstantString(expr.prettyName.toUpperCase) +
            blockStatement(convertStatements(fields, expr.children:_*))

        case UnaryMinus(child) =>
          ConstantString("-") +
            blockStatement(convertStatement(child, fields))

        case Pow(left, right) =>
          ConstantString("POWER") +
            blockStatement(convertStatement(left, fields) + "," + convertStatement(right, fields))

        case PromotePrecision(child) => convertStatement(child, fields)

        case CheckOverflow(child, t) =>
          MiscStatement.getCastType(t) match {
            case Some(cast) =>
              ConstantString("CAST") +
                blockStatement(convertStatement(child, fields) + "AS" + cast)
            case _ => convertStatement(child, fields)
          }

        case Pi() => ConstantString("PI()") !

        case Rand(seed) =>
          ConstantString("RANDOM") + blockStatement(LongVariable(seed.asInstanceOf[Long])!)
        case Round(child, scale) =>
          ConstantString("ROUND") + blockStatement(convertStatements(fields, child, scale))

        case _ => null
      }
    )
  }
}
