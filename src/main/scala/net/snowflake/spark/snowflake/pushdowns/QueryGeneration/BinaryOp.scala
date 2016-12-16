package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode}

/**
  * Created by ema on 12/15/16.
  */
private[snowflake] object BinaryOp {
  def unapply(node: BinaryNode): Option[(LogicalPlan, LogicalPlan)] = {
    Option(node) map {
      n => (n.left, n.right)
    }
  }
}
