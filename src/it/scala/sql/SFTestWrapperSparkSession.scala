package org.apache.spark.sql.thundersnow

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

class SFTestWrapperSparkSession(sc: SparkContext,
                                sfConfigs: Map[String, String]) extends SparkSession(sc) {

  val formatName = net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_SHORT_NAME;

  /**
   * Random suffix appended appended to table and directory names in order to avoid collisions.
   */
  protected def randomSuffix: String = Math.abs(Random.nextLong()).toString

  override def createDataFrame[A <: Product: TypeTag](data: Seq[A]): DataFrame = {
    val origDF = super.createDataFrame(data)
    val tableName = s"tempTestTable_$randomSuffix"
    origDF.write.format(formatName).options(sfConfigs).option("dbtable", tableName).save()
    read.format("snowflake").options(sfConfigs).option("dbtable", tableName).load()
  }
}

object SFTestWrapperSparkSession {

  def apply(context: SparkContext, configs: Map[String, String]): SFTestWrapperSparkSession =
    new SFTestWrapperSparkSession(context, configs)
}
