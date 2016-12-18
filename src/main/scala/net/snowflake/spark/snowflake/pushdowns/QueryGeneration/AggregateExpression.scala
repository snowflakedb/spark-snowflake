package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions._

private[snowflake] object AggregateExpression {

  def unapply(expAttr: (Expression, Seq[Attribute])): Option[String] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    // Take only the first child, as all of the functions below have only one.
    expr.children.headOption.flatMap(agg_fun => {
      val fn_name = agg_fun.prettyName.trim.toLowerCase
      (fn_name match {
        case "avg" | "max" | "min" | "sum" => Some(fn_name)
        case _                             => None
      }).flatMap(name =>
        agg_fun.children.headOption.map(child =>
          name + block(convertExpression(child, fields))))
    })
  }
}
