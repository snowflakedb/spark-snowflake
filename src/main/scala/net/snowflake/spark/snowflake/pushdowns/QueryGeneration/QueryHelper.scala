package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import net.snowflake.spark.snowflake.SnowflakePushdownException
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  NamedExpression
}

/**
  * Created by ema on 12/16/16.
  */
private[snowflake] case class QueryHelper(
    children: Seq[SnowflakeQuery],
    projections: Option[Seq[NamedExpression]] = None,
    outputAttributes: Option[Seq[Attribute]],
    alias: String,
    conjunction: String) {

  val output: Seq[Attribute] = {
    projections.map(p => p.map(_.toAttribute)).getOrElse {

      if (children.isEmpty) {
        outputAttributes.getOrElse(throw new SnowflakePushdownException(
          "Query output attributes must not be empty when it has no children."))

      } else
        children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)
    }
  }

  val outputWithQualifier = output.map(
    a =>
      AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
        a.exprId,
        Some(alias)))

  val colSet = children.foldLeft(Seq.empty[Attribute])((x, y) =>
    x ++ y.helper.outputWithQualifier)

  val columns: Option[String] = projections map { p =>
    p.map(e => convertExpression(e, colSet)).mkString(", ")
  }

  val source =
    if (children.nonEmpty)
      children
        .map(c => c.getQuery(useAlias = true))
        .mkString(s""" $conjunction """)
    else conjunction
}
