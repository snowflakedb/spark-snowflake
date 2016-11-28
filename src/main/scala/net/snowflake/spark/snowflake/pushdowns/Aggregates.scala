package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.expressions._

object Aggregates {

  def unapply(expr: Expression): Option[(String, Expression)] = {

    val agg_fun = expr.children(0)
    val fn_name = agg_fun.prettyName.trim.toLowerCase //TODO:improve string parsing logic

    fn_name match {
      case "avg" |
           "max"  |
           "min" |
           "sum" => Some(fn_name, agg_fun.children(0))

      case _ => None
    }
  }
}

