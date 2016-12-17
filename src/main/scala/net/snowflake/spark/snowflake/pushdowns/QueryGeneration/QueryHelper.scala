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

  lazy val output = {
    projections.map(p => p.map((_.toAttribute))).getOrElse {

      if (children.isEmpty) {
        outputAttributes.getOrElse(throw new SnowflakePushdownException(
          "Query output attributes must not be empty when it has no children."))

      } else
        children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)
    }
  }

  lazy val outputWithQualifier = output.map(
    a =>
      AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
        a.exprId,
        Some(alias)))

  lazy val colSet = children.foldLeft(Seq.empty[Attribute])((x, y) =>
    x ++ y.helper.outputWithQualifier)

  lazy val columns: Option[String] = projections map { p =>
    p.map(e => convertExpression(e, colSet)).mkString(", ")
  }

  lazy val source = children
    .map(c => c.getQuery(true))
    .mkString(if (children.isEmpty) conjunction else "",
              s""" $conjunction """,
              "")
}
