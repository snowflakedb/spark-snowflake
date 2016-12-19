package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Contains,
  EndsWith,
  Expression,
  In,
  InSet,
  IsNotNull,
  IsNull,
  Literal,
  StartsWith
}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

/**
  * Extractor for boolean expressions (return true or false).
  */
private[QueryGeneration] object BooleanExpression {

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
            list.map(l => convertExpression(l, fields)).mkString(", "))

        case IsNull(child) =>
          block(convertExpression(child, fields) + " IS NULL")
        case IsNotNull(child) =>
          block(convertExpression(child, fields) + " IS NOT NULL")

        case Contains(child, Literal(v: UTF8String, StringType)) =>
          convertExpression(child, fields) + " LIKE " + s"%${v.toString}%"
        case EndsWith(child, Literal(v: UTF8String, StringType)) =>
          convertExpression(child, fields) + " LIKE " + s"%${v.toString}"
        case StartsWith(child, Literal(v: UTF8String, StringType)) =>
          convertExpression(child, fields) + " LIKE " + s"${v.toString}%"

        case _ => null
      }
    )
  }
}
