package org.apache.spark.sql.snowflake

import net.snowflake.spark.snowflake.Utils
import org.apache.spark.SparkContext
import org.apache.spark.sql.classic.{DataFrame => ClassicDataFrame, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.Encoder

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

/** Spark 4: public [[org.apache.spark.sql.SparkSession]] has no `(sc)` ctor; extend classic impl. */
class SFTestWrapperSparkSession(sc: SparkContext, sfConfigs: Map[String, String])
    extends ClassicSparkSession(sc) {

  def seqToDF[T: Encoder](data: Seq[T]): ClassicDataFrame = {
    createSnowflakeDFFromSparkDF(super.createDataset(data).toDF())
  }

  def seqToDF[T: Encoder](data: Seq[T], colNames: String*): ClassicDataFrame = {
    createSnowflakeDFFromSparkDF(super.createDataset(data).toDF(colNames: _*))
  }

  private def createSnowflakeDFFromSparkDF(df: ClassicDataFrame): ClassicDataFrame = {
    val tableName = s"tempTestTable_${Math.abs(Random.nextLong()).toString}"
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

  override def createDataFrame[A <: Product: TypeTag](data: Seq[A]): ClassicDataFrame = {
    createSnowflakeDFFromSparkDF(super.createDataFrame(data))
  }
}

object SFTestWrapperSparkSession {

  def apply(context: SparkContext, configs: Map[String, String]): SFTestWrapperSparkSession =
    new SFTestWrapperSparkSession(context, configs)
}
