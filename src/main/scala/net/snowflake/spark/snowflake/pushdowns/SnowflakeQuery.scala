package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.SnowflakeRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by ema on 11/30/16.
  */

object SnowflakeQuery {

  def fromPlan(plan: LogicalPlan): Option[QueryBuilder] = {
    new QueryBuilder(plan).tryBuild
  }
}

abstract class SnowflakeQuery {

  val output: Seq[Attribute]
  val alias: String
  val child: Option[SnowflakeQuery]

  def selectedColumns: String = child match {
    case Some(q: SnowflakeQuery) => "asdf"
    case None => "*"
  }

  def getQuery: String = {

    val source = child match {
      case Some(q: SnowflakeQuery) => q.getQuery
      case None => ""
    }

    s"SELECT $selectedColumns FROM ($source) AS $alias"
  }

  def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse({
      if(child.isEmpty)
        None
      else
        child.get.find(query)
    })
}

case class SourceQuery(relation: SnowflakeRelation, alias: String) extends SnowflakeQuery {

  // No child subquery for SourceQueries
  override val child = None

  val cluster = relation.params.sfURL + "/" + relation.params.sfWarehouse + "/" + relation.params.sfDatabase
  val tableOrQuery = relation.params.query.getOrElse(relation.params.table.get)
  val query: SQLBuilder = SQLBuilder.fromStatic(tableOrQuery.toString)

  override def getQuery: String = tableOrQuery.toString

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] = query.lift(this)

  override def prettyPrint(depth: Int, builder: StringBuilder): Unit =
    builder
      .indent(depth)
      .append(s"BaseQuery[$alias, ${output.mkString(",")}] (")
      .append(query.sql)
      .append(")\n")

}

class DummyQuery extends SnowflakeQuery {
  override val output = None
  override val alias = "foo"
  override val child = None
}