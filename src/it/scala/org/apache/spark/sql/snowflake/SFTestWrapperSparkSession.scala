package org.apache.spark.sql.snowflake

import net.snowflake.spark.snowflake.{TestUtils, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class SFTestWrapperSparkSession(sc: SparkContext, sfConfigs: Map[String, String])
    extends SparkSession(sc) {

  override def createDataFrame[A <: Product: TypeTag](data: Seq[A]): DataFrame = {
    // Create a DataFrame from the Seq using Spark's normal way.
    val origDF = super.createDataFrame(data)

    // Write this DataFrame to Snowflake, in effect testing the connector's write-out
    // code path, and then read it back as a Snowflake DataFrame. This uses
    // the temporary schema defined in the run properties, tempTestSchema
    val tableName = s"tempTestTable_${TestUtils.randomSuffix}"

    origDF.write
      .format(Utils.SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(sfConfigs)
      .option("dbtable", tableName)
      .save()
    read
      .format(Utils.SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(sfConfigs)
      .option("dbtable", tableName)
      .load()
  }
}

object SFTestWrapperSparkSession {

  def apply(context: SparkContext, configs: Map[String, String]): SFTestWrapperSparkSession =
    new SFTestWrapperSparkSession(context, configs)
}
