package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql. Strategy

class SnowflakeStrategy extends Strategy{

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    val cleanedPlan = plan.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })

    try {
      SnowflakePlan.buildQueryRDD(cleanedPlan).getOrElse(Nil)
    } catch {
      case e: MatchError => {
        logDebug(s"Failed to match plan: $e")
        Nil
      }
    }
  }
}

