package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.SnowflakeRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by ema on 11/30/16.
  */

object SnowflakeQuery {

  def fromPlan(plan: LogicalPlan): Option[QueryBuilder] = {
    new QueryBuilder(plan).tryBuild
  }

   def renameExpressions(columnAliases: Iterator[String],
                        expressions: Seq[NamedExpression]): Seq[NamedExpression] =
    expressions.map {
      // We need to special case Alias, since this is not valid SQL:
      // select (foo as bar) as baz from ...
      case a@Alias(child: Expression, name: String) => {
        val metadata = MetadataUtils.preserveOriginalName(a)
        Alias(child, columnAliases.next)(a.exprId, None, Some(metadata))
      }
      case expr: NamedExpression => {
        val metadata = MetadataUtils.preserveOriginalName(expr)
        Alias(expr, columnAliases.next)(expr.exprId, None, Some(metadata))
      }
    }

}

abstract class SnowflakeQuery {

  val output: Seq[Attribute]
  val alias: String
  val child: SnowflakeQuery

  val fields: Seq[Attribute] = child.qualifiedOutput

  def qualifiedOutput: Seq[Attribute] = output.map(
    a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, Some(alias.toString)))

  def selectedColumns: String =  {
      "*"
  }

  val suffix: String = ""

  def getQuery: String = {
    val source = if(child == null) {
      ""
    } else {
      child.getQuery
    }

    s"SELECT $selectedColumns FROM ($source) $suffix AS $alias"
  }

  def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse({
      if(child == null)
        None
      else
        child.find(query)
    })

  def attr(a: Attribute): String = {
    NamedExpression
    fields.find(e => e.exprId == a.exprId) match {
      case Some(resolved) =>
        "\"" + resolved.qualifier + "." + resolved.name + "\""
      case None =>
        "\"" + a.qualifier + "." + a.name + "\""
    }
  }

  def expressionToString(expression: Expression): String = {
    expression match {
      case a: Attribute => attr(a)
    /*  case l: Literal => raw(l.toString()).param(l)
      case Alias(child: Expression, name: String) =>
        block { addExpression(child) }.raw(" AS ").identifier(name)

      case Cast(child, t) => t match {
        case TimestampType | DateType => block {
          raw("UNIX_TIMESTAMP")
          block { addExpression(child) }
          raw(" * 1000")
        }
        case _ => TypeConversions.DataFrameTypeToSnowflakeCastType(t) match {
          case None => addExpression(child)
          case Some(t) => {
            raw("CAST").block {
              addExpression(child)
                .raw(" AS ")
                .raw(t)
            }
          } */
        }

      }

  def sharesCluster(otherTree: SnowflakeQuery): Boolean = {
    val result = for {
      myBase <- find { case q: SourceQuery => q }
      otherBase <- otherTree.find { case q: SourceQuery => q }
    } yield {
      myBase.cluster == otherBase.cluster
    }
    result.getOrElse(false)
  }
}

case class SourceQuery(relation: SnowflakeRelation, alias: String) extends SnowflakeQuery {

  // No child subquery for SourceQueries
  override val child = null
  override val output = relation.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  override val fields = output

  val cluster = relation.params.sfURL + "/" + relation.params.sfWarehouse + "/" + relation.params.sfDatabase
  val tableOrQuery = relation.params.query.getOrElse(relation.params.table.get)
  val query: SQLBuilder = SQLBuilder.fromStatic(tableOrQuery.toString)

  override def getQuery: String = tableOrQuery.toString

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] = query.lift(this)
}

case class FilterQuery(condition: Expression, child: SnowflakeQuery, alias: String) extends SnowflakeQuery {
  override val output = child.output
  override val suffix = "WHERE " + expressionToString(condition)
}

case class ProjectQuery(projectionColumns: Seq[NamedExpression],
                        columnAliases: Iterator[String],
                        child: SnowflakeQuery,
                        alias: String) extends SnowflakeQuery {

  override val output = SnowflakeQuery.
    renameExpressions(columnAliases, projectionColumns).map(_.toAttribute)

  override def selectedColumns: String = {
    var columnList = ""
    if (output.isEmpty) {
      columnList = "*"
    } else {
      for(i <- output.indices) {
        if(i != 0) {
          columnList += ", "
        }
        columnList += expressionToString(output(i))
      }
    }
   columnList
  }
}

case class AggregateQuery(projectionColumns: Seq[NamedExpression],
                          groups: Seq[Expression],
                        columnAliases: Iterator[String],
                        child: SnowflakeQuery,
                        alias: String) extends SnowflakeQuery {

  override val output = SnowflakeQuery.
    renameExpressions(columnAliases, projectionColumns).map(_.toAttribute)

  override def selectedColumns: String = {
    var columnList = ""
    if (output.isEmpty) {
      columnList = "count(*)"
    } else {
      for(i <- output.indices) {
        if(i != 0) {
          columnList += ", "
        }
        columnList += expressionToString(output(i))
      }
    }
    columnList
  }

  override val suffix = "GROUP BY " +
    groups.map(group => expressionToString(group)).mkString(",")
}

case class JoinQuery(columnAliases: Iterator[String],
                     left: SnowflakeQuery,
                     right: SnowflakeQuery,
                     conditions: Seq[Expression],
                     alias: String) extends SnowflakeQuery {

  override val child = null

  override val fields: Seq[Attribute] = {
    left.qualifiedOutput ++ right.qualifiedOutput
  }

  override val output = SnowflakeQuery.
    renameExpressions(columnAliases, fields).map(_.toAttribute)

  override def selectedColumns: String = {
    var columnList = ""
    if (output.isEmpty) {
      columnList = "*"
    } else {
      for(i <- output.indices) {
        if(i != 0) {
          columnList += ", "
        }
        columnList += expressionToString(output(i))
      }
    }
    columnList
  }

  override def getQuery: String = {
    val source = left.getQuery +
    " INNER JOIN " + right.getQuery

    s"SELECT $selectedColumns FROM ($source) $suffix AS $alias"
  }

  override val suffix = " ON " +
    conditions.map(cond => expressionToString(cond)).mkString(",")
}

class DummyQuery extends SnowflakeQuery {
  override val child = null;
  override val alias = "foo";
  override val output = null;
}