package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{EmptySnowflakeSQLStatement, SnowflakePushdownException, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}

/**
  * Helper class to maintain the fields, output, and projection expressions of
  * a SnowflakeQuery. This may be refactored into the SnowflakeQuery abstract class.
  *
  * @constructor Creates an instance of a QueryHelper. Created by every SnowflakeQuery.
  * @param children A sequence containing the child queries. May be empty in the case
  *                 of a source (bottom-level) query, contain one element (for most
  *                 unary operations), or contain two elements (for joins, etc.).
  * @param projections Contains optional projection columns for this query.
  * @param outputAttributes Optional manual override for output.
  * @param alias The alias for this subquery.
  * @param conjunction Conjunction phrase to be used in between subquery children, or simple phrase
  *                    when there are no subqueries.
  */
private[querygeneration] case class QueryHelper(
    children: Seq[SnowflakeQuery],
    projections: Option[Seq[NamedExpression]] = None,
    outputAttributes: Option[Seq[Attribute]],
    alias: String,
    conjunction: String = "",
    fields: Option[Seq[Attribute]] = None,
    conjunctionStatement: SnowflakeSQLStatement = EmptySnowflakeSQLStatement()) {

  val colSet =
    if (fields.isEmpty)
      children.foldLeft(Seq.empty[Attribute])((x, y) =>
        x ++ y.helper.outputWithQualifier)
    else fields.get

  val pureColSet =
    children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)

  val processedProjections = projections
    .map(p =>
      p.map(e =>
        colSet.find(c => c.exprId == e.exprId) match {
          case Some(a) =>
            AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
              a.exprId,
              None)
          case None => e
      }))
    .map(p => renameColumns(p, alias))

  val columns: Option[String] = processedProjections map { p =>
    p.map(e => convertExpression(e, colSet)).mkString(", ")
  }

  val output: Seq[Attribute] = {
    outputAttributes.getOrElse(
      processedProjections.map(p => p.map(_.toAttribute)).getOrElse {
        if (children.isEmpty) {
          throw new SnowflakePushdownException(
            "Query output attributes must not be empty when it has no children.")
        } else
          children.foldLeft(Seq.empty[Attribute])((x, y) =>
            x ++ y.helper.output)
      }
    )
  }

  val outputWithQualifier = output.map(
    a =>
      AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
        a.exprId,
        Some(alias)))

  val source =
    if (children.nonEmpty)
      children
        .map(c => c.getQuery(useAlias = true))
        .mkString(s""" $conjunction """)
    else conjunction

  val sourceStatement: SnowflakeSQLStatement =
    if(children.nonEmpty)
      mkStatement(children, conjunctionStatement)
    else conjunctionStatement
}
