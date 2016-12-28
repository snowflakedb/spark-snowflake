package net.snowflake.spark.snowflake.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.plans.logical._

/**
  * Extractor for supported unary operations.
  */
private[querygeneration] object UnaryOp {

  def unapply(node: UnaryNode): Option[LogicalPlan] =
    node match {
      case _: Filter | _: Project | _: GlobalLimit | _: LocalLimit |
          _: Aggregate | _: Sort | _: ReturnAnswer =>
        Some(node.child)

      case _ => None
    }
}
