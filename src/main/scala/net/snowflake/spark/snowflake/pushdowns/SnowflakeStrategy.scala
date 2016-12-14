package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy

class SnowflakeStrategy extends Strategy {

  /* Clean up the plan, then try to generate a query from it for Snowflake.
   * Try-Catch is unnecessary, but in beta mode we'll do it for safety and let Spark use other strategies
   * in case of failure.
   */
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      SnowflakePlan
        .buildQueryRDD(plan.transform({
          case Project(Nil, child) => child
          case SubqueryAlias(_, child) => child
        }))
        .getOrElse(Nil)
    } catch {
      case e: Exception => Nil
    }
  }
}
