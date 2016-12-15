package net.snowflake.spark.snowflake.pushdowns.SQLExpressions

import net.snowflake.spark.snowflake.pushdowns.SQLGenerator
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, IsNotNull, IsNull}

/**
  * Created by ema on 12/15/16.
  */
object BooleanExpression extends SQLGenerator {

  def unapply(expAttr: (Expression, Seq[Attribute])): Option[String] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    expr match {
      case IsNotNull(child) =>
        Some(block(convertExpression(child, fields) + " IS NOT NULL"))
      case IsNull(child) =>
        Some(block(convertExpression(child, fields) + " IS NULL"))

      case _ => None
    }
  }
}
