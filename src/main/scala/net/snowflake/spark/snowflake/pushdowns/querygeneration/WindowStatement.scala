package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
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
      // Handle Window Expression.
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

          // Disable window function pushdown if
          // 1. The function are both window function and aggregate function
          // 2. The function support Window Frame
          // 3. The user specify ORDER-BY clause
          case agg: AggregateExpression
            if AggregationStatement.supportWindowFrame(agg) && spec.orderSpec.nonEmpty =>
            null

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

      // PercentRank can't be pushdown to snowflake because:
      //   1. Snowflake's percent_rank only supports window frame type: RANGE.
      //   2. Spark's PercentRank only supports window frame type: ROWS
      case _: PercentRank => null

      case _ => null
    })
  }

  // Handle window block.
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
