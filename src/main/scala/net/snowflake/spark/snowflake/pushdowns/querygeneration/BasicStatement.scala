package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryOperator, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, Expression, Literal, Or}
import org.apache.spark.sql.types._

/**
  * Extractor for basic (attributes and literals) expressions.
  */
private[querygeneration] object BasicStatement {

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

    Option(expr match {
      case a: Attribute => addAttributeStatement(a, fields)
      case And(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "AND" + convertStatement(right, fields))
      case Or(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "OR" + convertStatement(right, fields))
      case BitwiseAnd(left, right) =>
        ConstantString("BITAND") + blockStatement(convertStatements(fields, left, right))
      case BitwiseOr(left, right) =>
        ConstantString("BITOR") + blockStatement(convertStatements(fields, left, right))
      case BitwiseXor(left, right) =>
        ConstantString("BITXOR") + blockStatement(convertStatements(fields, left, right))
      case BitwiseNot(child) =>
        ConstantString("BITNOT") + blockStatement(convertStatement(child, fields))
      case b: BinaryOperator =>
        blockStatement(
          convertStatement(b.left, fields) + b.symbol + convertStatement(b.right, fields)
        )
      case l: Literal =>
        l.dataType match {
          case StringType => {
            val str = l.toString()
            if (str == "null") ConstantString(str.toUpperCase) !
            else StringVariable(str) !    //else "'" + str + "'"
          }
          case DateType =>
            ConstantString("DATEADD(day,") + IntVariable(l.value.asInstanceOf[Int]) +
          ", TO_DATE('1970-01-01'))" //s"DATEADD(day, ${l.value}, TO_DATE('1970-01-01'))"
          case TimestampType =>  ConstantString("to_timestamp_ntz(") + l.toString() + ", 6)"
          case _ =>
            l.value match {
              case v: Int => IntVariable(v) !
              case v: Long => LongVariable(v) !
              case v: Short => ShortVariable(v) !
              case v: Boolean => BooleanVariable(v) !
              case v: Float => FloatVariable(v) !
              case v: Double => DoubleVariable(v) !
              case v: Byte => ByteVariable(v) !
              case _ => ConstantString(l.value.toString) !
            }
        }

      case _ => null
    })
  }
}