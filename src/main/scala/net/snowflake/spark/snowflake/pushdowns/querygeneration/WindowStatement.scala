package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  DenseRank,
  Expression,
  PercentRank,
  Rank,
  RowNumber,
  WindowExpression,
  WindowSpecDefinition
}

/**
  * Windowing functions
  */
private[querygeneration] object WindowStatement {

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      // Handle Window Expression. (It is moved from MiscStatement)
      case WindowExpression(func, spec) =>
        func match {
          // These functions in Snowflake support a window frame.
          // Note that pushdown for these may or may not yet be supported in the connector.
          case _: Rank | _: DenseRank | _: PercentRank =>
            convertStatement(func, fields) + " OVER " + windowBlock(
              spec,
              fields,
              useWindowFrame = true
            )

          // These do not.
          case _ =>
            convertStatement(func, fields) + " OVER " + windowBlock(
              spec,
              fields,
              useWindowFrame = false
            )
        }

      // Handle supported window function
      case _: RowNumber | _: Rank | _: DenseRank =>
        ConstantString(expr.prettyName.toUpperCase) + "()"

      // Until July 21 2020, PercentRank can't be pushdown to snowflake.
      //   1. Snowflake's percent_rank only supports window frame type: RANGE.
      //   2. Spark's PercentRank only supports window frame type: ROWS
      case _: PercentRank => null

      case _ => null
    })
  }

  // Handle window block. (It is moved from MiscStatement)
  private final def windowBlock(
                                 spec: WindowSpecDefinition,
                                 fields: Seq[Attribute],
                                 useWindowFrame: Boolean
                               ): SnowflakeSQLStatement = {
    val partitionBy =
      if (spec.partitionSpec.isEmpty) {
        EmptySnowflakeSQLStatement()
      } else {
        ConstantString("PARTITION BY") +
          mkStatement(spec.partitionSpec.map(convertStatement(_, fields)), ",")
      }

    val orderBy =
      if (spec.orderSpec.isEmpty) {
        EmptySnowflakeSQLStatement()
      } else {
        ConstantString("ORDER BY") +
          mkStatement(spec.orderSpec.map(convertStatement(_, fields)), ",")
      }

    val fromTo =
      if (!useWindowFrame || spec.orderSpec.isEmpty) ""
      else " " + spec.frameSpecification.sql

    blockStatement(partitionBy + orderBy + fromTo)
  }

}
