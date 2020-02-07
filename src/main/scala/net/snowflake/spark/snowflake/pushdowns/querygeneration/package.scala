package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{
  ConstantString,
  EmptySnowflakeSQLStatement,
  SnowflakeSQLStatement
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.types.MetadataBuilder
import org.slf4j.LoggerFactory

/** Package-level static methods and variable constants. These includes helper functions for
  * adding and converting expressions, formatting blocks and identifiers, logging, and
  * formatting SQL.
  */
package object querygeneration {

  private[querygeneration] final val ORIG_NAME = "ORIG_NAME"

  /** This wraps all identifiers with the following symbol. */
  private final val identifier = "\""

  private[querygeneration] final val log = LoggerFactory.getLogger(getClass)

  private[querygeneration] final def blockStatement(
    stmt: SnowflakeSQLStatement
  ): SnowflakeSQLStatement =
    ConstantString("(") + stmt + ")"

  private[querygeneration] final def blockStatement(
    stmt: SnowflakeSQLStatement,
    alias: String
  ): SnowflakeSQLStatement =
    blockStatement(stmt) + "AS" + wrapStatement(alias)

  /** This adds an attribute as part of a SQL expression, searching in the provided
    * fields for a match, so the subquery qualifiers are correct.
    *
    * @param attr The Spark Attribute object to be added.
    * @param fields The Seq[Attribute] containing the valid fields to be used in the expression,
    *               usually derived from the output of a subquery.
    * @return A SnowflakeSQLStatement representing the attribute expression.
    */
  private[querygeneration] final def addAttributeStatement(
    attr: Attribute,
    fields: Seq[Attribute]
  ): SnowflakeSQLStatement =
    fields.find(e => e.exprId == attr.exprId) match {
      case Some(resolved) =>
        qualifiedAttributeStatement(resolved.qualifier, resolved.name)
      case None => qualifiedAttributeStatement(attr.qualifier, attr.name)
    }

  /** Qualifies identifiers with that of the subquery to which it belongs */
  private[querygeneration] final def qualifiedAttribute(
    alias: Option[String],
    name: String
  ): String = {
    val str =
      if (alias.isEmpty) ""
      else alias.map(wrap).mkString(".") + "."

    if (name.startsWith("\"") && name.endsWith("\"")) str + name
    else str + wrapObjectName(name)
  }

  private[querygeneration] final def qualifiedAttributeStatement(
    alias: Option[String],
    name: String
  ): SnowflakeSQLStatement =
    ConstantString(qualifiedAttribute(alias, name)) !

  private[querygeneration] final def wrapObjectName(name: String): String =
    globalParameter match {
      case Some(params) =>
        identifier + (if (params.keepOriginalColumnNameCase) name
                      else name.toUpperCase()) + identifier
      case _ => wrap(name)
    }

  private[querygeneration] final def wrap(name: String): String = {
    identifier + name.toUpperCase + identifier
  }

  private[querygeneration] final def wrapStatement(
    name: String
  ): SnowflakeSQLStatement =
    ConstantString(identifier + name.toUpperCase + identifier) !

  /** This performs the conversion from Spark expressions to SQL runnable by Snowflake.
    * We should have as many entries here as possible, or the translation will not be
    * able ot happen.
    *
    * @note (A MatchError may be raised for unsupported Spark expressions).
    */
  private[querygeneration] final def convertStatement(
    expression: Expression,
    fields: Seq[Attribute]
  ): SnowflakeSQLStatement = {
    (expression, fields) match {
      case AggregationStatement(stmt) => stmt
      case BasicStatement(stmt) => stmt
      case BooleanStatement(stmt) => stmt
      case DateStatement(stmt) => stmt
      case MiscStatement(stmt) => stmt
      case NumericStatement(stmt) => stmt
      case StringStatement(stmt) => stmt
      case WindowStatement(stmt) => stmt
    }
  }

  private[querygeneration] final def convertStatements(
    fields: Seq[Attribute],
    expressions: Expression*
  ): SnowflakeSQLStatement =
    mkStatement(expressions.map(convertStatement(_, fields)), ",")

  private[querygeneration] def renameColumns(
    origOutput: Seq[NamedExpression],
    alias: String
  ): Seq[NamedExpression] = {

    val col_names = Iterator.from(0).map(n => s"COL_$n")

    origOutput.map { expr =>
      val metadata =
        if (!expr.metadata.contains(ORIG_NAME)) {
          new MetadataBuilder()
            .withMetadata(expr.metadata)
            .putString(ORIG_NAME, expr.name)
            .build
        } else expr.metadata

      val altName = s"""${alias}_${col_names.next()}"""

      expr match {
        case a @ Alias(child: Expression, name: String) =>
          Alias(child, altName)(a.exprId, None, Some(metadata))
        case _ =>
          Alias(expr, altName)(expr.exprId, None, Some(metadata))
      }
    }
  }

  /** This takes a query in the shape produced by QueryBuilder and
    * performs the necessary indentation for pretty printing.
    *
    * @note Warning: This is a hacky implementation that isn't very 'functional' at all.
    * In fact, it's quite imperative.
    *
    * This is useful for logging and debugging.
    */
  private[querygeneration] final def prettyPrint(query: String): String = {
    log.debug(s"""Attempting to prettify query $query...""")

    val opener = "\\(SELECT"
    val closer = "\\) AS \\\"SUBQUERY_[0-9]{1,10}\\\""

    val breakPoints = "(" + "(?=" + opener + ")" + "|" + "(?=" + closer + ")" +
      "|" + "(?<=" + closer + ")" + ")"

    var remainder = query
    var indent = 0

    val str = new StringBuilder
    var inSuffix: Boolean = false

    while (remainder.length > 0) {
      val prefix = "\n" + "\t" * indent
      val parts = remainder.split(breakPoints, 2)
      str.append(prefix + parts.head)

      if (parts.length >= 2 && parts.last.length > 0) {
        val n: Char = parts.last.head

        if (n == '(') {
          indent += 1
        } else {

          if (!inSuffix) {
            indent -= 1
            inSuffix = true
          }

          if (n == ')') {
            inSuffix = false
          }
        }
        remainder = parts.last
      } else remainder = ""
    }

    str.toString()
  }

  final def mkStatement(
    seq: Seq[SnowflakeSQLStatement],
    delimiter: SnowflakeSQLStatement
  ): SnowflakeSQLStatement =
    seq.foldLeft(EmptySnowflakeSQLStatement()) {
      case (left, stmt) =>
        if (left.isEmpty) stmt else left + delimiter + stmt
    }

  final def mkStatement(seq: Seq[SnowflakeSQLStatement],
                        delimiter: String): SnowflakeSQLStatement =
    mkStatement(seq, ConstantString(delimiter) !)
}
