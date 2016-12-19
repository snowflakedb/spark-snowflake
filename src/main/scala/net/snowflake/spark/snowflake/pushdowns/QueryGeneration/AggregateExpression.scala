package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions._

/**
  * Extractor for aggregate-style expressions.
  */
private[QueryGeneration] object AggregateExpression {

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

    // Take only the first child, as all of the functions below have only one.
    expr.children.headOption.flatMap(agg_fun => {
      val fn_name = agg_fun.prettyName.trim.toLowerCase
      Option(fn_name match {
        case "avg" | "max" | "min" | "sum" => fn_name
        case _                             => null
      }).flatMap(name =>
        agg_fun.children.headOption.map(child =>
          name + block(convertExpression(child, fields))))
    })
  }
}
