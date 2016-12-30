package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

/**
  * SnowflakePlan, with RDD defined by custom query.
  */
case class SnowflakePlan(output: Seq[Attribute], rdd: RDD[InternalRow])
    extends SparkPlan {

  override def children: Seq[SparkPlan]                = Nil
  protected override def doExecute(): RDD[InternalRow] = rdd

}