package net.snowflake.spark.snowflake.pushdowns

import net.snowflake.spark.snowflake.{SnowflakeConnectorFeatureNotSupportException, Utils}
import net.snowflake.spark.snowflake.pushdowns.querygeneration.QueryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

/** Clean up the plan, then try to generate a query from it for Snowflake.
  * The Try-Catch is unnecessary and may obfuscate underlying problems,
  * but in beta mode we'll do it for safety and let Spark use other strategies
  * in case of unexpected failure.
  */
class SnowflakeStrategy extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      buildQueryRDD(plan.transform({
        case Project(Nil, child) => child
        case SubqueryAlias(_, child) => child
      })).getOrElse(Nil)
    } catch {
      case ue: SnowflakeConnectorFeatureNotSupportException =>
        log.warn(s"Snowflake does't support this feature :${ue.getMessage}")
        throw ue
      case e: Exception =>
        log.warn(s"Pushdown failed :${e.getMessage}")
        Nil
    }
  }

  /** Attempts to get a SparkPlan from the provided LogicalPlan.
    *
    * @param plan The LogicalPlan provided by Spark.
    * @return An Option of Seq[SnowflakePlan] that contains the PhysicalPlan if
    *         query generation was successful, None if not.
    */
  private def buildQueryRDD(plan: LogicalPlan): Option[Seq[SparkPlan]] = {
    if (Utils.lazyMode) {
      logInfo("Using lazy mode for push down")
      QueryBuilder.getSnowflakeScanExecPlan(plan).map {
        case (projection, snowflakeSQL, relation) =>
          Seq(SnowflakeScanExec(projection, snowflakeSQL, relation))
      }
    } else {
      QueryBuilder.getRDDFromPlan(plan).map {
        case (output: Seq[Attribute], rdd: RDD[InternalRow]) =>
          Seq(SnowflakePlan(output, rdd))
      }
    }
  }
}
