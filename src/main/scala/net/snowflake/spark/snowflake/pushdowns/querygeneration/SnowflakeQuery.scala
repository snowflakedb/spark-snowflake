package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Cast,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Building blocks of a translated query, with nested subqueries. */
private[querygeneration] abstract sealed class SnowflakeQuery {

  /** Output columns. */
  lazy val output: Seq[Attribute] =
    if (helper == null) Seq.empty
    else {
      helper.output
        .map { col =>
          val orig_name =
            if (col.metadata.contains(ORIG_NAME)) {
              col.metadata.getString(ORIG_NAME)
            } else col.name

          Alias(Cast(col, col.dataType), orig_name)(
            col.exprId,
            None,
            Some(col.metadata)
          )
        }
        .map(_.toAttribute)
    }

  val helper: QueryHelper

  /** What comes after the FROM clause. */
  val suffixStatement: SnowflakeSQLStatement = EmptySnowflakeSQLStatement()

  def expressionToStatement(expr: Expression): SnowflakeSQLStatement =
    convertStatement(expr, helper.colSet)

  /** Converts this query into a String representing the SQL.
    *
    * @param useAlias Whether or not to alias this translated block of SQL.
    * @return SQL statement for this query.
    */
  def getStatement(useAlias: Boolean = false): SnowflakeSQLStatement = {
    log.debug(s"""Generating a query of type: ${getClass.getSimpleName}""")

    val stmt =
      ConstantString("SELECT") + helper.columns.getOrElse(ConstantString("*") !) + "FROM" +
        helper.sourceStatement + suffixStatement

    if (useAlias) {
      blockStatement(stmt, helper.alias)
    } else {
      stmt
    }
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
  * @param relation   The base SnowflakeRelation representing the basic table, view,
  *                   or subquery defined by the user.
  * @param refColumns Columns used to override the output generation for the QueryHelper.
  *                   These are the columns resolved by SnowflakeRelation.
  * @param alias      Query alias.
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
    conjunctionStatement = blockStatement(
      relation.params.query
        .map(ConstantString("(") + _ + ")") // user input query, don't parse
        .getOrElse(relation.params.table.get.toStatement !),
      "sf_connector_query_alias"
    )
  )

  /** Triplet that defines the Snowflake cluster that houses this base relation.
    * Currently an exact match on cluster is needed for a join, but we may not need
    * to be this strict.
    */
  val cluster: (String, Option[String], String) = (
    relation.params.sfURL,
    relation.params.sfWarehouse,
    relation.params.sfDatabase
  )

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this)
}

/** The query for a filter operation.
  *
  * @constructor
  * @param conditions The filter condition.
  * @param child      The child node.
  * @param alias      Query alias.
  */
case class FilterQuery(conditions: Seq[Expression],
                       child: SnowflakeQuery,
                       alias: String,
                       fields: Option[Seq[Attribute]] = None)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias,
      fields = fields
    )

  override val suffixStatement: SnowflakeSQLStatement =
    ConstantString("WHERE") + mkStatement(
      conditions.map(expressionToStatement),
      "AND"
    )
}

/** The query for a projection operation.
  *
  * @constructor
  * @param columns The projection columns.
  * @param child   The child node.
  * @param alias   Query alias.
  */
case class ProjectQuery(columns: Seq[NamedExpression],
                        child: SnowflakeQuery,
                        alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = Some(columns),
      outputAttributes = None,
      alias = alias
    )
}

/** The query for a aggregation operation.
  *
  * @constructor
  * @param columns The projection columns, containing also the aggregate expressions.
  * @param groups  The grouping columns.
  * @param child   The child node.
  * @param alias   Query alias.
  */
case class AggregateQuery(columns: Seq[NamedExpression],
                          groups: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = if (columns.isEmpty) None else Some(columns),
      outputAttributes = None,
      alias = alias
    )

  override val suffixStatement: SnowflakeSQLStatement =
    if (groups.nonEmpty) {
      ConstantString("GROUP BY") +
        mkStatement(groups.map(expressionToStatement), ",")
    } else {
      EmptySnowflakeSQLStatement()
    }
}

/** The query for Sort and Limit operations.
  *
  * @constructor
  * @param limit   Limit expression.
  * @param orderBy Order By expressions.
  * @param child   The child node.
  * @param alias   Query alias.
  */
case class SortLimitQuery(limit: Option[Expression],
                          orderBy: Seq[Expression],
                          child: SnowflakeQuery,
                          alias: String)
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias
    )

  override val suffixStatement: SnowflakeSQLStatement = {
    val statementFirstPart =
      if (orderBy.nonEmpty) {
        ConstantString("ORDER BY") + mkStatement(
          orderBy.map(expressionToStatement),
          ","
        )
      } else {
        EmptySnowflakeSQLStatement()
      }

    statementFirstPart + limit
      .map(ConstantString("LIMIT") + expressionToStatement(_))
      .getOrElse(EmptySnowflakeSQLStatement())
  }
}

/** The query for join operations.
  *
  * @constructor
  * @param left       The left query subtree.
  * @param right      The right query subtree.
  * @param conditions The join conditions.
  * @param joinType   The join type.
  * @param alias      Query alias.
  */
case class JoinQuery(left: SnowflakeQuery,
                     right: SnowflakeQuery,
                     conditions: Option[Expression],
                     joinType: JoinType,
                     alias: String)
    extends SnowflakeQuery {

  val conj: String = joinType match {
    case Inner =>
      // keep the nullability for both projections
      "INNER JOIN"
    case LeftOuter =>
      // Update the column's nullability of right table as true
      right.helper.outputWithQualifier =
        right.helper.nullableOutputWithQualifier
      "LEFT OUTER JOIN"
    case RightOuter =>
      // Update the column's nullability of left table as true
      left.helper.outputWithQualifier =
        left.helper.nullableOutputWithQualifier
      "RIGHT OUTER JOIN"
    case FullOuter =>
      // Update the column's nullability of both tables as true
      left.helper.outputWithQualifier =
        left.helper.nullableOutputWithQualifier
      right.helper.outputWithQualifier =
        right.helper.nullableOutputWithQualifier
      "FULL OUTER JOIN"
    case _ => throw new MatchError
  }

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left, right),
      projections = Some(
        left.helper.outputWithQualifier ++ right.helper.outputWithQualifier
      ),
      outputAttributes = None,
      alias = alias,
      conjunctionStatement = ConstantString(conj) !
    )

  override val suffixStatement: SnowflakeSQLStatement =
    conditions
      .map(ConstantString("ON") + expressionToStatement(_))
      .getOrElse(EmptySnowflakeSQLStatement())

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}

case class LeftSemiJoinQuery(left: SnowflakeQuery,
                             right: SnowflakeQuery,
                             conditions: Option[Expression],
                             isAntiJoin: Boolean = false,
                             alias: Iterator[String])
    extends SnowflakeQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left),
      projections = Some(left.helper.outputWithQualifier),
      outputAttributes = None,
      alias = alias.next
    )

  val cond: Seq[Expression] =
    if (conditions.isEmpty) Seq.empty else Seq(conditions.get)

  val anti: String = if (isAntiJoin) " NOT " else " "

  override val suffixStatement: SnowflakeSQLStatement =
    ConstantString("WHERE") + anti + "EXISTS" + blockStatement(
      FilterQuery(
        conditions = cond,
        child = right,
        alias = alias.next,
        fields = Some(
          left.helper.outputWithQualifier ++ right.helper.outputWithQualifier
        )
      ).getStatement()
    )

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}

/** The query for union.
  *
  * @constructor
  * @param children Children of the union expression.
  */
case class UnionQuery(children: Seq[LogicalPlan],
                      alias: String,
                      outputCols: Option[Seq[Attribute]] = None)
    extends SnowflakeQuery {

  val queries: Seq[SnowflakeQuery] = children.map { child =>
    new QueryBuilder(child).treeRoot
  }

  override val helper: QueryHelper =
    QueryHelper(
      children = queries,
      outputAttributes = Some(queries.head.helper.output),
      alias = alias
    )

  override def getStatement(useAlias: Boolean): SnowflakeSQLStatement = {
    val query =
      if (queries.nonEmpty) {
        mkStatement(
          queries.map(c => blockStatement(c.getStatement())),
          "UNION ALL"
        )
      } else {
        EmptySnowflakeSQLStatement()
      }

    if (useAlias) {
      blockStatement(query, alias)
    } else {
      query
    }
  }

  override def find[T](query: PartialFunction[SnowflakeQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        queries
          .map(q => q.find(query))
          .view
          .foldLeft[Option[T]](None)(_ orElse _)
      )
}

/** Query including a windowing clause.
  *
  * @constructor
  * @param windowExpressions The windowing expressions.
  * @param child             The child query.
  * @param alias             Query alias.
  * @param fields            The fields generated by this query referenceable by the parent.
  */
case class WindowQuery(windowExpressions: Seq[NamedExpression],
                       child: SnowflakeQuery,
                       alias: String,
                       fields: Option[Seq[Attribute]])
    extends SnowflakeQuery {

  val projectionVector: Seq[NamedExpression] =
    windowExpressions ++ child.helper.outputWithQualifier

  // We need to reorder the projections based on the output vector
  val orderedProjections: Option[Seq[NamedExpression]] =
    fields.map(_.map(reference => {
      val origPos = projectionVector.map(_.exprId).indexOf(reference.exprId)
      projectionVector(origPos)
    }))

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = orderedProjections,
      outputAttributes = None,
      alias = alias
    )
}
