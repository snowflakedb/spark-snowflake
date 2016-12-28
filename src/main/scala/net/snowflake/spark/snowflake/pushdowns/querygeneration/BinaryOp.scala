package net.snowflake.spark.snowflake.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.plans.logical.{
  BinaryNode,
  Join,
  LogicalPlan
}

/**
  * Extractor for binary logical operations (e.g., joins).
  */
private[querygeneration] object BinaryOp {

  def unapply(node: BinaryNode): Option[(LogicalPlan, LogicalPlan)] =
    Option(node match {
      case _: Join => (node.left, node.right)
      case _       => null
    })
}
