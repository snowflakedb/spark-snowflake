package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

/**
  * Created by ema on 12/15/16.
  */
private[snowflake] object Unary {
  def unapply(node: UnaryNode): Option[LogicalPlan] = {
    Option(node) map {
      n => n.child
    }
  }
}
