package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.slf4j.LoggerFactory

/**
  * Package-level static methods and variable constants. These includes helper functions for
  * adding and converting expressions, formatting blocks and identifiers, logging, and
  * formatting SQL.
  */
package object QueryGeneration {

  /** This wraps all identifiers with the following symbol. */
  private final val identifier = "\""

  private[QueryGeneration] final val log = LoggerFactory.getLogger(getClass)

  /** Query blocks. */
  private[QueryGeneration] final def block(text: String): String = {
    "(" + text + ")"
  }

  /** Same as block() but with an alias. */
  private[QueryGeneration] final def block(text: String,
                                           alias: String): String = {
    "(" + text + ") AS " + wrap(alias)
  }

  /** This adds an attribute as part of a SQL expression, searching in the provided
    * fields for a match, so the subquery qualifiers are correct.
    *
    * @param attr The Spark Attribute object to be added.
    * @param fields The Seq[Attribute] containing the valid fields to be used in the expression,
    *               usually derived from the output of a subquery.
    * @return A string representing the attribute expression.
    */
  private[QueryGeneration] final def addAttribute(
      attr: Attribute,
      fields: Seq[Attribute]): String = {
    fields.find(e => e.exprId == attr.exprId) match {
      case Some(resolved) =>
        qualifiedAttribute(resolved.qualifier, resolved.name)
      case None => qualifiedAttribute(attr.qualifier, attr.name)
    }
  }

  /** Qualifies identifiers with that of the subquery to which it belongs */
  private[QueryGeneration] final def qualifiedAttribute(alias: Option[String],
                                                        name: String) = {
    val str = alias match {
      case Some(qualifier) => wrap(qualifier) + "."
      case None            => ""
    }

    str + wrap(name)
  }

  private[QueryGeneration] final def wrap(name: String): String = {
    identifier + name + identifier
  }

  /** This performs the conversion from Spark expressions to SQL runnable by Snowflake.
    * We should have as many entries here as possible, or the translation will not be able ot happen.
    *
    * @note (A MatchError may be raised for unsupported Spark expressions).
    */
  private[QueryGeneration] final def convertExpression(
      expression: Expression,
      fields: Seq[Attribute]): String = {
    (expression, fields) match {
      case AggregationExpression(sql) => sql
      case BasicExpression(sql)       => sql
      case BooleanExpression(sql)     => sql
      case DateExpression(sql)        => sql
      case MiscExpression(sql)        => sql
      case NumericExpression(sql)     => sql
      case StringExpression(sql)      => sql
    }
  }

  private[QueryGeneration] final def convertExpressions(
      fields: Seq[Attribute],
      expressions: Expression*): String = {
    expressions.map(e => convertExpression(e, fields)).mkString(", ")
  }

  /** This takes a query in the shape produced by QueryBuilder and
    * performs the necessary indentation for pretty printing.
    *
    * @note Warning: This is a hacky implementation that isn't very 'functional' at all.
    * In fact, it's quite imperative.
    *
    * This is useful for logging and debugging.
    */
  private[QueryGeneration] final def prettyPrint(query: String): String = {
    log.debug(s"""Attempting to prettify query $query...""")

    val opener = "\\(SELECT"
    val closer = "\\) AS \\\"subquery_[0-9]{1,10}\\\""

    val breakPoints = "(" + "(?=" + opener + ")" + "|" + "(?=" + closer + ")" +
        "|" + "(?<=" + closer + ")" + ")"

    var remainder = query
    var indent    = 0

    val str               = new StringBuilder
    var inSuffix: Boolean = false

    while (remainder.length > 0) {
      val prefix = "\n" + "\t" * indent
      val parts  = remainder.split(breakPoints, 2)
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
}
