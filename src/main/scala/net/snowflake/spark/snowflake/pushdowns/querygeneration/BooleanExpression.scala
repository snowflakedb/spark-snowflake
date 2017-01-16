package net.snowflake.spark.snowflake.pushdowns.querygeneration

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
private[querygeneration] object BooleanExpression {

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
        case In(child, list) if list.forall(_.isInstanceOf[Literal]) =>
          convertExpression(child, fields) + " IN " + block(
            convertExpressions(fields, list: _*))

        case IsNull(child) =>
          block(convertExpression(child, fields) + " IS NULL")
        case IsNotNull(child) =>
          block(convertExpression(child, fields) + " IS NOT NULL")

        case Not(child) => {
          child match {
            case EqualTo(left, right) =>
              block(
                convertExpression(left, fields) + " != " + convertExpression(
                  right,
                  fields))
            case GreaterThanOrEqual(left, right) =>
              convertExpression(LessThan(left, right), fields)
            case LessThanOrEqual(left, right) =>
              convertExpression(GreaterThan(left, right), fields)
            case GreaterThan(left, right) =>
              convertExpression(LessThanOrEqual(left, right), fields)
            case LessThan(left, right) =>
              convertExpression(GreaterThanOrEqual(left, right), fields)
            case _ => "NOT" + block(convertExpression(child, fields))
          }
        }

        case Contains(child, Literal(pattern: UTF8String, StringType)) =>
          convertExpression(child, fields) + " LIKE " + s"'%${pattern.toString}%'"
        case EndsWith(child, Literal(pattern: UTF8String, StringType)) =>
          convertExpression(child, fields) + " LIKE " + s"'%${pattern.toString}'"
        case StartsWith(child, Literal(pattern: UTF8String, StringType)) =>
          convertExpression(child, fields) + " LIKE " + s"'${pattern.toString}%'"

        case _ => null
      }
    )
  }
}
