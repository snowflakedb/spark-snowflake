package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.pushdowns.SQLExpressions.{
  AggregateExpression,
  BasicExpression,
  BooleanExpression,
  MiscExpression
}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

/**
  * Created by ema on 12/15/16.
  */
trait SQLGenerator {
  private val identifier = "\""

  protected final def wrap(name: String): String = {
    identifier + name + identifier
  }

  final def block(text: String): String = {
    "(" + text + ")"
  }

  // Aliased block
  final def block(text: String, alias: String): String = {
    "(" + text + ") AS " + wrap(alias)
  }

  final def aliasedAttribute(alias: Option[String], name: String) = {
    val str = alias match {
      case Some(qualifier) => wrap(qualifier) + "."
      case None            => ""
    }

    str + wrap(name)
  }

  final def addAttribute(a: Attribute, fields: Seq[Attribute]): String = {
    fields.find(e => e.exprId == a.exprId) match {
      case Some(resolved) => aliasedAttribute(resolved.qualifier, resolved.name)
      case None           => aliasedAttribute(a.qualifier, a.name)
    }
  }

  /** This performs the conversion from Spark expressions to SQL runnable by Snowflake.
    * We should have as many entries here as possible, or the translation will not be able ot happen.
    *
    * @note (A MatchError will be raised for unsupported Spark expressions).
    */
  def convertExpression(expression: Expression, fields: Seq[Attribute]): String = {
    (expression, fields) match {
      case BasicExpression(sql)     => sql
      case BooleanExpression(sql)   => sql
      case AggregateExpression(sql) => sql
      case MiscExpression(sql)      => sql
    }
  }
}
