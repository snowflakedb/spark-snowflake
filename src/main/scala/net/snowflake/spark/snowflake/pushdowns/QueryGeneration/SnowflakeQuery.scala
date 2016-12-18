package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import net.snowflake.spark.snowflake.SnowflakeRelation
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  NamedExpression
}

/**
  * Created by ema on 11/30/16.
  */
private[snowflake] abstract sealed class SnowflakeQuery {

  lazy val output: Seq[Attribute] =
    if (helper == null) Seq.empty
    else helper.output

  val helper: QueryHelper
  val suffix: String = ""

  def expressionToString(expr: Expression): String = {
    convertExpression(expr, helper.colSet)
  }

  def getQuery(useAlias: Boolean = false): String = {
    val query =
      s"""SELECT ${helper.columns.getOrElse("*")} FROM ${helper.source}$suffix"""

    if (useAlias)
      block(query) + s""" AS "${helper.alias}""""
    else query
  }

  def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        if (helper.children.isEmpty) None
        else helper.children.head.find(query)
      )

  def sharesCluster(otherTree: SnowflakeQuery): Boolean = {
    val result = for {
      myBase <- find { case q: SourceQuery => q }
      otherBase <- otherTree.find {
        case q: SourceQuery => q
      }
    } yield {
      myBase.cluster == otherBase.cluster
    }

    result.getOrElse(false)
  }
}

case class SourceQuery(relation: SnowflakeRelation,
                       refColumns: Seq[Attribute],
                       alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper = QueryHelper(
    Seq.empty,
    None,
    Some(refColumns),
    alias,
    relation.params.query.getOrElse(relation.params.table.get.toString))

  val cluster = (relation.params.sfURL,
                 relation.params.sfWarehouse,
                 relation.params.sfDatabase)

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this)
}

case class FilterQuery(condition: Expression,
                       child: SnowflakeQuery,
                       alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(Seq(child), None, None, alias, "")

  override val suffix = " WHERE " + expressionToString(condition)
}

case class ProjectQuery(columns: Seq[NamedExpression],
                        child: SnowflakeQuery,
                        alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(Seq(child), Some(columns), None, alias, "")
}

case class AggregateQuery(columns: Seq[NamedExpression],
                          groups: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(Seq(child), Some(columns), None, alias, "")

  override val suffix = " GROUP BY " + groups
      .map(group => expressionToString(group))
      .mkString(",")
}

case class SortLimitQuery(limit: Expression,
                          orderBy: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(Seq(child), None, None, alias, "")

  override val suffix = {
    val order_clause =
      if (orderBy.nonEmpty)
        " GROUP BY " + orderBy.map(e => expressionToString(e)).mkString(", ")
      else ""

    order_clause + " LIMIT " + expressionToString(limit)
  }
}

case class JoinQuery(left: SnowflakeQuery,
                     right: SnowflakeQuery,
                     conditions: Option[Expression],
                     alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(Seq(left, right), None, None, alias, "INNER JOIN")

  override val suffix = {
    val str = conditions match {
      case Some(e) => " ON "
      case None    => ""
    }
    str + conditions.map(cond => expressionToString(cond)).mkString(",")
  }

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}
