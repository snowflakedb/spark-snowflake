package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.SnowflakeRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.slf4j.LoggerFactory

/**
  * Created by ema on 11/30/16.
  */
private[snowflake] object SnowflakeQuery {

  final val log =
    LoggerFactory.getLogger(getClass)
}

private[snowflake] abstract sealed class SnowflakeQuery extends SQLGenerator {

  lazy val queryType = getClass.getSimpleName
  val output: Seq[Attribute]
  val alias: String
  val child: SnowflakeQuery
  val columns: Seq[NamedExpression] = Seq.empty

  val fields: Seq[Attribute] =
    if (child != null) child.qualifiedOutput
    else null
  val suffix: String = ""
  val source =
    if (child == null) ""
    else child.getQuery(true)

  def qualifiedOutput: Seq[Attribute] =
    output.map(a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, Some(alias.toString)))

  def getQuery(useAlias: Boolean = false): String = {
    SnowflakeQuery.log.debug(s"""Now generating the query for $queryType""")

    val query =
      s"SELECT $selectedColumns FROM $source$suffix"

    if (useAlias)
      block(query) + s""" AS "$alias""""
    else query
  }

  // Use for pretty printing.
  def getPrettyQuery(useAlias: Boolean = false, depth: Int = 0): String = {
    val indent = "\n" + ("\t" * depth)

    if (child == null) return indent + getQuery(true)

    val src = child.getPrettyQuery(true, depth + 1)

    val query =
      s"""${indent}SELECT $selectedColumns FROM $src$indent$suffix"""

    if (useAlias)
      block(query) + s"""$indent AS "$alias""""
    else query
  }

  def selectedColumns: String =
    if (columns.isEmpty) "*"
    else
      columns.map(expr => expressionToString(expr)).mkString(", ")

  def expressionToString(expression: Expression): String = {
    convertExpression(expression, fields)
  }

  def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        if (child == null) None
        else child.find(query)
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

case class SourceQuery(relation: SnowflakeRelation, output: Seq[Attribute], alias: String) extends SnowflakeQuery {

  // No child subquery for SourceQueries
  override val child  = null
  override val fields = output
  override val source =
    relation.params.query.getOrElse(relation.params.table.get.toString)
  val cluster = relation.params.sfURL + "/" + relation.params.sfWarehouse + "/" + relation.params.sfDatabase

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this)
}

case class FilterQuery(condition: Expression, child: SnowflakeQuery, alias: String) extends SnowflakeQuery {
  override val output = child.output
  override val suffix = " WHERE " + expressionToString(condition)
}

case class ProjectQuery(override val columns: Seq[NamedExpression], child: SnowflakeQuery, alias: String)
    extends SnowflakeQuery {

  override val output = columns.map(_.toAttribute)
}

case class AggregateQuery(override val columns: Seq[NamedExpression],
                          groups: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String)
    extends SnowflakeQuery {

  override val output = columns.map(_.toAttribute)
  override val suffix = " GROUP BY " + groups.map(group => expressionToString(group)).mkString(",")

  override def selectedColumns: String = {
    if (columns.isEmpty) "count(*)"
    else
      columns.map(expr => expressionToString(expr)).mkString(", ")
  }
}

case class SortLimitQuery(limit: Expression, orderBy: Seq[Expression], child: SnowflakeQuery, alias: String)
    extends SnowflakeQuery {

  override val output = child.output
  override val suffix = {
    val order_clause =
      if (!orderBy.isEmpty)
        " GROUP BY " + orderBy.map(e => expressionToString(e)).mkString(", ")
      else ""

    order_clause + " LIMIT " + expressionToString(limit)
  }
}

case class JoinQuery(left: SnowflakeQuery, right: SnowflakeQuery, conditions: Option[Expression], alias: String)
    extends SnowflakeQuery {

  override val child = null

  override val fields: Seq[Attribute] = {
    left.qualifiedOutput ++ right.qualifiedOutput
  }

  override val output = fields

  override val source = left.getQuery(true) + " INNER JOIN " + right.getQuery(true)
  override val suffix = {
    val str = conditions match {
      case Some(e) => " ON "
      case None    => ""
    }
    str + conditions.map(cond => expressionToString(cond)).mkString(",")
  }

  // Use for pretty printing.
  override def getPrettyQuery(useAlias: Boolean = false, depth: Int = 0): String = {
    val indent = "\n" + ("\t" * depth)

    val l = left.getPrettyQuery(true, depth + 1)
    val r = right.getPrettyQuery(true, depth + 1)

    val query =
      s"""${indent}SELECT $selectedColumns FROM $l$indent INNER JOIN $r $indent$suffix"""

    if (useAlias)
      block(query) + s"""$indent AS "$alias""""
    else query
  }

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}
