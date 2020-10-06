package org.apache.spark.sql.snowflake

import net.snowflake.spark.snowflake.Utils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class SFTestWrapperSparkSession(sc: SparkContext, sfConfigs: Map[String, String])
    extends SparkSession(sc) {

  private def createSnowflakeDFFromSparkDF(df: DataFrame): DataFrame = {
    // Write this DataFrame to Snowflake, in effect testing the connector's write-out
    // code path, and then read it back as a Snowflake DataFrame. This uses
    // the temporary schema defined in the run properties, tempTestSchema
    val tableName = s"tempTestTable_${Utils.randomSuffix}"
    df.write
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

  def seqToDF[T: Encoder](data: Seq[T]): DataFrame = {
    createSnowflakeDFFromSparkDF(super.createDataset(data).toDF())
  }

  def seqToDF[T: Encoder](data: Seq[T], colNames: String*): DataFrame = {
    createSnowflakeDFFromSparkDF(super.createDataset(data).toDF(colNames: _*))
  }

  override def createDataFrame[A <: Product: TypeTag](data: Seq[A]): DataFrame = {
    createSnowflakeDFFromSparkDF(super.createDataFrame(data))
  }
}

object SFTestWrapperSparkSession {

  def apply(context: SparkContext, configs: Map[String, String]): SFTestWrapperSparkSession =
    new SFTestWrapperSparkSession(context, configs)
}
