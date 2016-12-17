package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan}

/**
  * Created by ema on 12/15/16.
  */
private[snowflake] object BinaryOp {
  def unapply(node: BinaryNode): Option[(LogicalPlan, LogicalPlan)] = {
    node match {
      case _: Join => Some((node.left, node.right))
      case _       => None
    }
  }
}