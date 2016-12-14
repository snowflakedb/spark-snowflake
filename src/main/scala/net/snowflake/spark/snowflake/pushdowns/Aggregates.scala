package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

object Aggregates {

  def unapply(expr: Expression): Option[(String, Expression)] = {

    // Take only the first child, as all of the functions below have only one.
    expr.children.lift(0).flatMap(
      agg_fun => {
        val fn_name = agg_fun.prettyName.trim.toLowerCase
        (fn_name match {
          case "avg"
               | "max"
               | "min"
               | "sum" => Some(fn_name)
          case _ => None
        }).flatMap(f => agg_fun.children.lift(0).map(child => (f, child) ))
      })
  }
}
