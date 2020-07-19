package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Contains,
  EndsWith,
  EqualTo,
  Expression,
  GreaterThan,
  GreaterThanOrEqual,
  In,
  IsNotNull,
  IsNull,
  LessThan,
  LessThanOrEqual,
  Literal,
  Not,
  StartsWith
}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object BooleanStatement {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case In(child, list) if list.forall(_.isInstanceOf[Literal]) => {
        list match {
          case Nil =>
            ConstantString("false").toStatement
          case _ =>
            convertStatement(child, fields) + "IN" +
              blockStatement(convertStatements(fields, list: _*))
        }
      }
      case IsNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NULL")
      case IsNotNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NOT NULL")
      case Not(child) => {
        child match {
          case EqualTo(left, right) =>
            blockStatement(
              convertStatement(left, fields) + "!=" +
                convertStatement(right, fields)
            )
          case GreaterThanOrEqual(left, right) =>
            convertStatement(LessThan(left, right), fields)
          case LessThanOrEqual(left, right) =>
            convertStatement(GreaterThan(left, right), fields)
          case GreaterThan(left, right) =>
            convertStatement(LessThanOrEqual(left, right), fields)
          case LessThan(left, right) =>
            convertStatement(GreaterThanOrEqual(left, right), fields)
          case _ =>
            ConstantString("NOT") +
              blockStatement(convertStatement(child, fields))
        }
      }
      case Contains(child, Literal(pattern: UTF8String, StringType)) =>
        convertStatement(child, fields) + "LIKE" + s"'%${pattern.toString}%'"
      case EndsWith(child, Literal(pattern: UTF8String, StringType)) =>
        convertStatement(child, fields) + "LIKE" + s"'%${pattern.toString}'"
      case StartsWith(child, Literal(pattern: UTF8String, StringType)) =>
        convertStatement(child, fields) + "LIKE" + s"'${pattern.toString}%'"

      case _ => null
    })
  }
}
