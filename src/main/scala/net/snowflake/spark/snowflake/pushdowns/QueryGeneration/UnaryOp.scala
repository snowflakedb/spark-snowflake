package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.plans.logical._

/**
  * Created by ema on 12/15/16.
  */
private[snowflake] object UnaryOp {
  def unapply(node: UnaryNode): Option[LogicalPlan] = {
      node match {
        case _: Filter
          | _: Project
          | _: GlobalLimit
          | _: LocalLimit
          | _: Aggregate
          | _: Sort
          => Some(node.child)

        case _ => None
    }
  }
}
