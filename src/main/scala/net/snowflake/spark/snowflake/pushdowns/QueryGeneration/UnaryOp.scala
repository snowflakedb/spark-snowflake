package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.plans.logical._

/**
  * Extractor for supported unary operations.
  */
private[QueryGeneration] object UnaryOp {

  def unapply(node: UnaryNode): Option[LogicalPlan] =
    node match {
      case _: Filter | _: Project | _: GlobalLimit | _: LocalLimit |
          _: Aggregate | _: Sort | _: ReturnAnswer =>
        Some(node.child)

      case _ => None
    }
}
