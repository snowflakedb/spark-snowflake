package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.expressions._
import StringBuilderImplicits._
import net.snowflake.spark.snowflake.SnowflakeRelation

abstract class AbstractQuery {
  def alias: QueryAlias
  def output: Seq[Attribute]
  def collapse: SQLBuilder

  def qualifiedOutput: Seq[Attribute] = output.map(
    a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, Some(alias.toString)))

  def castedNamedOutput: Seq[NamedExpression] = output.map(
    a => Alias(
      Cast(a, a.dataType),
      MetadataUtils.getOriginalName(a).getOrElse(a.name)
    )(a.exprId, None, Some(a.metadata))
  )

  /**
    * Performs a pre-order traversal of the query tree
    * and returns the first non-None result from the query
    * function.
    */
  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T]
  def findAll[T](query: PartialFunction[AbstractQuery, T]): Option[Seq[T]]

  def prettyPrint(depth: Int, builder: StringBuilder): Unit

  def objToOpt[T](obj: Seq[Any]) = {
    obj.length match {
      case 0 => None
      case _ => Some(obj.map{x => x.asInstanceOf[T]})
    }
  }

  def buildFindSeq(opt: Option[Any]) : Seq[Any] = {
    opt match {
      case Some(elems) => {
        if(elems.isInstanceOf[Seq[Any]]) elems.asInstanceOf[Seq[Any]]
        else Seq(elems)
      }
      case None => Seq.empty
    }
  }

  def sharesCluster(otherTree: AbstractQuery): Boolean = {
    val result = for {
      myBase <- find { case q: BaseQuery => q }
      otherBase <- otherTree.find { case q: BaseQuery => q }
    } yield {
      myBase.cluster == otherBase.cluster
    }
    result.getOrElse(false)
  }
}

case class BaseQuery(alias: QueryAlias, relation: SnowflakeRelation, output: Seq[Attribute]) extends AbstractQuery {

  //val prefix = "SELECT * FROM "

  val cluster = relation.params.sfURL + "/" + relation.params.sfWarehouse + "/" + relation.params.sfDatabase

  val tableOrQuery = relation.params.query.getOrElse(relation.params.table.get)

  val query: SQLBuilder = SQLBuilder.fromStatic(tableOrQuery.toString)

  override def collapse: SQLBuilder =
  SQLBuilder.withAlias(alias, b => b.appendBuilder(query))

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] = query.lift(this)

  def findAll[T](query: PartialFunction[AbstractQuery, T]): Option[Seq[T]] = {
    val res = buildFindSeq(query.lift(this))
    objToOpt[T](res)
  }

  override def prettyPrint(depth: Int, builder: StringBuilder): Unit =
    builder
      .indent(depth)
      .append(s"BaseQuery[$alias, ${output.mkString(",")}] (")
      .append(query.sql)
      .append(")\n")

}

case class PartialQuery(alias: QueryAlias,
                        output: Seq[Attribute],
                        prefix: Option[SQLBuilder] = None,
                        suffix: Option[SQLBuilder] = None,
                        inner: AbstractQuery) extends AbstractQuery {

  override def collapse: SQLBuilder = {
    val a = SQLBuilder.withAlias(alias, b => {
      b.raw("SELECT ")
        .maybeAppendBuilder(prefix, "*")
        .raw(" FROM ")
        .appendBuilder(inner.collapse)
        .maybeAppendBuilder(suffix)
    })
    val c = a.sql.toString()
    a
  }

  override def prettyPrint(depth: Int, builder: StringBuilder): Unit = {
    builder
      .indent(depth)
      .append(s"PartialQuery[$alias, ${output.mkString(",")}] (")
      .append(prefix.map(_.sql).getOrElse(""))
      .append(") (")
      .append(suffix.map(_.sql).getOrElse(""))
      .append(")\n")
    inner.prettyPrint(depth + 1, builder)
  }

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] =
    query.lift(this).orElse(inner.find(query))

  def findAll[T](query: PartialFunction[AbstractQuery, T]): Option[Seq[T]] = {
    val res = buildFindSeq(inner.findAll(query)) ++
      buildFindSeq(query.lift(this))
    objToOpt[T](res)
  }
}

case class JoinQuery(alias: QueryAlias,
                     output: Seq[Attribute],
                     projection: SQLBuilder,
                     condition: Option[SQLBuilder],
                     left: AbstractQuery,
                     right: AbstractQuery) extends AbstractQuery {

  override def collapse: SQLBuilder =
    SQLBuilder.withAlias(alias, b => {
      b.raw(s"SELECT ")
        .appendBuilder(projection)
        .raw(" FROM ")
        .appendBuilder(left.collapse)
        .raw(" INNER JOIN ")
        .appendBuilder(right.collapse)
        .raw(" ON ")
        .maybeAppendBuilder(condition, "1")
    })

  override def prettyPrint(depth: Int, builder: StringBuilder): Unit = {
    builder
      .indent(depth)
      .append(s"JoinQuery[$alias, ${output.mkString(",")}] (")
      .append(projection.sql)
      .append(") (")
      .append(condition.map(_.sql).getOrElse(""))
      .append(")\n")
    left.prettyPrint(depth + 1, builder)
    right.prettyPrint(depth + 1, builder)
  }

  def find[T](query: PartialFunction[AbstractQuery, T]): Option[T] =
    query.lift(this)
      .orElse(left.find(query))
      .orElse(right.find(query))

  def findAll[T](query: PartialFunction[AbstractQuery, T]): Option[Seq[T]] = {
    val res = buildFindSeq(left.findAll(query)) ++
      buildFindSeq(right.findAll(query)) ++
      buildFindSeq(query.lift(this))
    objToOpt[T](res)
  }

}