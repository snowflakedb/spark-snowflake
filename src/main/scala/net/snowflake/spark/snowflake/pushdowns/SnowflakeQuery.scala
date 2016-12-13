package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.SnowflakeRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, IsNotNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._

/**
  * Created by ema on 11/30/16.
  */

private[snowflake] object SnowflakeQuery {

  private final val identifier = "\""
  private final val DECIMAL_MAX_PRECISION = 65
  private final val DECIMAL_MAX_SCALE = 30

  final def fromPlan(plan: LogicalPlan): Option[QueryBuilder] = {
    new QueryBuilder(plan).tryBuild
  }

    protected final def wrap(name: String): String = {
    SnowflakeQuery.identifier + name + SnowflakeQuery.identifier
  }

  protected final def block(text: String): String = {
    "(" + text + ")"
  }

  // Aliased block
  protected final def block(text: String, alias: String): String = {
    "(" + text + ") AS " + wrap(alias)
  }

  def aliasedAttribute(alias: Option[String], name: String) = {
    val str = alias match {
      case Some(qualifier) => wrap(qualifier) + "."
      case None => ""
    }
    str + wrap(name)
  }

  def attr(a: Attribute, fields: Seq[Attribute]): String = {
    fields.find(e => e.exprId == a.exprId) match {
      case Some(resolved) =>
        aliasedAttribute(resolved.qualifier, resolved.name)
      case None =>
        aliasedAttribute(a.qualifier, a.name)
    }
  }

  def convertExpression(expression: Expression, fields: Seq[Attribute]): String = {
    expression match {
      case a: Attribute => attr(a,fields)
      case l: Literal => l.toString
      case Alias(child: Expression, name: String) =>
        block(convertExpression(child, fields), name)

      case Cast(child, t) =>
        SnowflakeQuery.getCastType(t) match {
          case None => convertExpression(child, fields)
          case Some(t) => {
            "CAST" + block(convertExpression(child, fields) + "AS " + t)
          }
        }

      case IsNotNull(child) => block(convertExpression(child, fields) + " IS NOT NULL")
      case Aggregates(name, child) => name + block(convertExpression(child, fields))
    }
  }

  final def renameExpressions(columnAliases: Iterator[String],
                              expressions: Seq[NamedExpression]): Seq[NamedExpression] =
    expressions.map {
      expr =>
        Alias(expr, columnAliases.next)(expr.exprId, None, Some(expr.metadata))
    }

  def decimalTypeToMySQLType(decimal: DecimalType): String = {
    val precision = Math.min(DECIMAL_MAX_PRECISION, decimal.precision)
    val scale = Math.min(DECIMAL_MAX_SCALE, decimal.scale)
    s"DECIMAL($precision, $scale)"
  }

  /**
    * Attempts a best effort conversion from a SparkType
    * to a Snowflake type to be used in a Cast.
    *
    * @note Will raise a match error for unsupported casts
    */
  protected final def getCastType(t: DataType): Option[String] = t match {
    case StringType => Some("VARCHAR")
    case BinaryType => Some("BINARY")
    case DateType => Some("DATE")
    case TimestampType => Some("DATE")
    case decimal: DecimalType => Some(decimalTypeToMySQLType(decimal))
    case LongType => Some("SIGNED")
    case FloatType => Some("DECIMAL(14, 7)")
    case DoubleType => Some("DECIMAL(30, 15)")
    case _ => None
  }
}

private[snowflake] abstract sealed class SnowflakeQuery {

  val output: Seq[Attribute]
  val alias: String
  val child: SnowflakeQuery

  val fields: Seq[Attribute] = if(child != null) child.qualifiedOutput else null

  def qualifiedOutput: Seq[Attribute] = output.map(
    a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, Some(alias.toString)))

  def selectedColumns: String = {
    if (output.isEmpty) {
      "*"
    } else {
      output.map(expr => expressionToString(expr)).mkString(", ")
    }
  }

  val suffix: String = ""

  val source =
    if (child == null) {
    ""
  } else {
    child.getQuery(true)
  }

  def getQuery(useAlias: Boolean = false): String = {
    val query =  s"SELECT $selectedColumns FROM $source $suffix"

    if(useAlias)
      SnowflakeQuery.block(query) + s""" AS "$alias""""
    else
      query
  }

  def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse({
      if (child == null)
        None
      else
        child.find(query)
    })

  def expressionToString(expression: Expression): String = {
    SnowflakeQuery.convertExpression(expression, fields)
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

case class SourceQuery(relation: SnowflakeRelation, output: Seq[Attribute], alias: String) extends SnowflakeQuery {

  // No child subquery for SourceQueries
  override val child = null
//  override val output = relation.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  override val fields = output

  val cluster = relation.params.sfURL + "/" + relation.params.sfWarehouse + "/" + relation.params.sfDatabase
  override val source = relation.params.query.getOrElse(relation.params.table.get.toString)

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
      for (i <- output.indices) {
        if (i != 0) {
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

case class SortLimitQuery(limit: Expression,
                          orderBy: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String) extends SnowflakeQuery {

  override val output = child.output
  override val suffix = {

    val order_clause = if (!orderBy.isEmpty) {
      "GROUP BY " + orderBy.map(e => expressionToString(e)).mkString(", ")
    } else {
      ""
    }

    order_clause + " LIMIT " + expressionToString(limit)
  }
}

case class JoinQuery(columnAliases: Iterator[String],
                     left: SnowflakeQuery,
                     right: SnowflakeQuery,
                     conditions: Option[Expression],
                     alias: String) extends SnowflakeQuery {

  override val child = null

  override val fields: Seq[Attribute] = {
    left.qualifiedOutput ++ right.qualifiedOutput
  }

  override val output = SnowflakeQuery.
    renameExpressions(columnAliases, fields).map(_.toAttribute)

  override val source = left.getQuery(true) +
      " INNER JOIN " + right.getQuery(true)

  override val suffix = {
    val str = conditions match {
      case Some(e) => " ON "
      case None => ""
    }
    str + conditions.map(cond => expressionToString(cond)).mkString(",")
  }
}

class DummyQuery extends SnowflakeQuery {
  override val child = null;
  override val alias = "foo";
  override val output = null;
}