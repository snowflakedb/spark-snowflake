package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.plans.logical.{
  BinaryNode,
  Join,
  LogicalPlan
}

/**
  * Extractor for binary logical operations (e.g., joins).
  */
private[QueryGeneration] object BinaryOp {

  def unapply(node: BinaryNode): Option[(LogicalPlan, LogicalPlan)] =
    node match {
      case _: Join => Some((node.left, node.right))
      case _       => None
    }
}
