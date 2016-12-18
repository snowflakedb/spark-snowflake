package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.pushdowns.QueryGeneration.QueryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

class SnowflakeStrategy extends Strategy {

  /* Clean up the plan, then try to generate a query from it for Snowflake.
   * Try-Catch is unnecessary, but in beta mode we'll do it for safety and let Spark use other strategies
   * in case of unexpected failure.
   */
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      buildQueryRDD(plan.transform({
        case Project(Nil, child)     => child
        case SubqueryAlias(_, child) => child
      })).getOrElse(Nil)
    } catch {
      case e: Exception => Nil
    }
  }

  private def buildQueryRDD(plan: LogicalPlan): Option[Seq[SnowflakePlan]] =
    QueryBuilder.getRDDFromPlan(plan).map {
      case (output: Seq[Attribute], rdd: RDD[InternalRow]) =>
        Seq(SnowflakePlan(output, rdd))
    }

}
