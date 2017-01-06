package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.SnowflakeRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans._

/** Building blocks of a translated query, with nested subqueries. */
private[querygeneration] abstract sealed class SnowflakeQuery {

  /** Output columns. */
  lazy val output: Seq[Attribute] =
    if (helper == null) Seq.empty
    else {
      helper.output.map { col =>
        val orig_name =
          if (col.metadata.contains(ORIG_NAME)) {
            col.metadata.getString(ORIG_NAME)
          } else col.name

        Alias(Cast(col, col.dataType), orig_name)(col.exprId,
                                                  None,
                                                  Some(col.metadata))
      }.map(_.toAttribute)
    }

  val helper: QueryHelper

  /** What comes after the FROM clause. */
  val suffix: String = ""

  def expressionToString(expr: Expression): String = {
    convertExpression(expr, helper.colSet)
  }

  /** Converts this query into a String representing the SQL.
    *
    * @param useAlias Whether or not to alias this translated block of SQL.
    * @return SQL statement for this query.
    */
  def getQuery(useAlias: Boolean = false): String = {
    log.debug(s"""Generating a query of type: ${getClass.getSimpleName}""")

    val query =
      s"""SELECT ${helper.columns.getOrElse("*")} FROM ${helper.source}$suffix"""

    if (useAlias)
      block(query) + s""" AS "${helper.alias}""""
    else query
  }

  /** Finds a particular query type in the overall tree.
    *
    * @param query PartialFunction defining a positive result.
    * @tparam T SnowflakeQuery type
    * @return Option[T] for one positive match, or None if nothing found.
    */
  def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        if (helper.children.isEmpty) None
        else helper.children.head.find(query)
      )

  /** Determines if two SnowflakeQuery subtrees can be joined together.
    *
    * @param otherTree The other tree, can it be joined with this one?
    * @return True if can be joined, or False if not.
    */
  def canJoin(otherTree: SnowflakeQuery): Boolean = {
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

/** The query for a base type (representing a table or view).
  *
  * @constructor
  * @param relation The base SnowflakeRelation representing the basic table, view, or subquery defined by
  *                 the user.
  * @param refColumns Columns used to override the output generation for the QueryHelper. These are the columns
  *                   resolved by SnowflakeRelation.
  * @param alias Query alias.
  */
case class SourceQuery(relation: SnowflakeRelation,
                       refColumns: Seq[Attribute],
                       alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper = QueryHelper(
    children = Seq.empty,
    projections = None,
    outputAttributes = Some(refColumns),
    alias = alias,
    conjunction =
      relation.params.query.getOrElse(relation.params.table.get.toString))

  /** Triplet that defines the Snowflake cluster that houses this base relation.
    * Currently an exact match on cluster is needed for a join, but we may not need
    * to be this strict.
    */
  val cluster = (relation.params.sfURL,
                 relation.params.sfWarehouse,
                 relation.params.sfDatabase)

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this)
}

/** The query for a filter operation.
  *
  * @constructor
  * @param condition The filter condition.
  * @param child The child node.
  * @param alias Query alias.
  */
case class FilterQuery(condition: Expression,
                       child: SnowflakeQuery,
                       alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(children = Seq(child),
                projections = None,
                outputAttributes = None,
                alias = alias)

  override val suffix = " WHERE " + expressionToString(condition)
}

/** The query for a projection operation.
  *
  * @constructor
  * @param columns The projection columns.
  * @param child The child node.
  * @param alias Query alias.
  */
case class ProjectQuery(columns: Seq[NamedExpression],
                        child: SnowflakeQuery,
                        alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(children = Seq(child),
                projections = Some(columns),
                outputAttributes = None,
                alias = alias)
}

/** The query for a aggregation operation.
  *
  * @constructor
  * @param columns The projection columns, containing also the aggregate expressions.
  * @param groups The grouping columns.
  * @param child The child node.
  * @param alias Query alias.
  */
case class AggregateQuery(columns: Seq[NamedExpression],
                          groups: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(children = Seq(child),
                projections = Some(columns),
                outputAttributes = None,
                alias = alias)

  override val suffix =
    if (!groups.isEmpty) {
      " GROUP BY " + groups
        .map(group => expressionToString(group))
        .mkString(", ")
    } else ""
}

/** The query for Sort and Limit operations.
  *
  * @constructor
  * @param limit Limit expression.
  * @param orderBy Order By expressions.
  * @param child The child node.
  * @param alias Query alias.
  */
case class SortLimitQuery(limit: Option[Expression],
                          orderBy: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(children = Seq(child),
                projections = None,
                outputAttributes = None,
                alias = alias)

  override val suffix = {
    val order_clause =
      if (orderBy.nonEmpty)
        " ORDER BY " + orderBy.map(e => expressionToString(e)).mkString(", ")
      else ""

    order_clause + limit
      .map(l => " LIMIT " + expressionToString(l))
      .getOrElse("")
  }
}

/** The query for join operations.
  *
  * @constructor
  * @param left The left query subtree.
  * @param right The right query subtree.
  * @param conditions The join conditions.
  * @param joinType The join type.
  * @param alias Query alias.
  */
case class JoinQuery(left: SnowflakeQuery,
                     right: SnowflakeQuery,
                     conditions: Option[Expression],
                     joinType: JoinType,
                     alias: String)
    extends SnowflakeQuery {


  val conj = joinType match {
    case Inner => "INNER JOIN"
    case LeftOuter => "LEFT OUTER JOIN"
    case RightOuter => "RIGHT OUTER JOIN"
    case FullOuter => "OUTER JOIN"
  }

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left, right),
      projections = Some(
        left.helper.outputWithQualifier ++ right.helper.outputWithQualifier),
      outputAttributes = None,
      alias = alias,
      conjunction = conj)

  override val suffix = {
    val str = conditions match {
      case Some(e) => " ON "
      case None    => ""
    }
    str + conditions.map(cond => expressionToString(cond)).mkString(" AND ")
  }

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}
