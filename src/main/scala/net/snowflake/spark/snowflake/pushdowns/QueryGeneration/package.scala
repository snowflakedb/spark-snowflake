package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.slf4j.LoggerFactory

/**
  * Created by ema on 12/15/16.
  */
package object QueryGeneration {
  private final val identifier     = "\""
  private[snowflake] final val log = LoggerFactory.getLogger(getClass)

  private[snowflake] final def block(text: String): String = {
    "(" + text + ")"
  }

  // Aliased block
  private[snowflake] final def block(text: String, alias: String): String = {
    "(" + text + ") AS " + wrap(alias)
  }

  private[snowflake] final def addAttribute(a: Attribute,
                                            fields: Seq[Attribute]): String = {
    fields.find(e => e.exprId == a.exprId) match {
      case Some(resolved) =>
        qualifiedAttribute(resolved.qualifier, resolved.name)
      case None => qualifiedAttribute(a.qualifier, a.name)
    }
  }

  private[snowflake] final def qualifiedAttribute(alias: Option[String],
                                                  name: String) = {
    val str = alias match {
      case Some(qualifier) => wrap(qualifier) + "."
      case None            => ""
    }

    str + wrap(name)
  }

  private[snowflake] final def wrap(name: String): String = {
    identifier + name + identifier
  }

  /** This performs the conversion from Spark expressions to SQL runnable by Snowflake.
    * We should have as many entries here as possible, or the translation will not be able ot happen.
    *
    * @note (A MatchError will be raised for unsupported Spark expressions).
    */
  private[snowflake] final def convertExpression(
      expression: Expression,
      fields: Seq[Attribute]): String = {
    (expression, fields) match {
      case BasicExpression(sql)     => sql
      case BooleanExpression(sql)   => sql
      case AggregateExpression(sql) => sql
      case MiscExpression(sql)      => sql
    }
  }
}
