package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by ema on 11/22/16.
  */
case class SnowflakePlan(builder: QueryBuilder) extends SparkPlan {

  override def output = builder.getOutput
  override def children: Seq[SparkPlan] = Nil

  protected override def doExecute(): RDD[InternalRow] =
    builder.physicalRDD

}

object SnowflakePlan {

  def buildQueryRDD(plan: LogicalPlan): Option[Seq[SnowflakePlan]] =
    new QueryBuilder(plan).tryBuild.map(builder => Seq(SnowflakePlan(builder)))

}
