package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  BinaryOperator,
  Expression,
  Literal
}

/**
  * Created by ema on 12/15/16.
  */
private[snowflake] object BasicExpression {

  def unapply(expAttr: (Expression, Seq[Attribute])): Option[String] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    expr match {
      case a: Attribute => Some(addAttribute(a, fields))
      case l: Literal   => Some(l.toString)
      case b: BinaryOperator =>
        Some(
          block(
            convertExpression(b.left, fields) + s"${b.symbol}" +
              convertExpression(b.right, fields)
          ))

      case _ => None
    }
  }
}
