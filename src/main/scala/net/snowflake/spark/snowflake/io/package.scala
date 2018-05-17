package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.SupportedSource.SupportedSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

package object io {

  def readRDD(
               sqlContext: SQLContext,
               params: MergedParameters,
               sql: String,
               jdbcWrapper: JDBCWrapper = DefaultJDBCWrapper,
               source: SupportedSource = SupportedSource.S3INTERNAL,
               format: SupportedFormat = SupportedFormat.CSV
             ): RDD[String] =
  {

    source match {
      case SupportedSource.S3INTERNAL =>
        new S3InternalRDD(sqlContext, params, sql, jdbcWrapper)
      case SupportedSource.S3EXTERNAL =>
        null
    }
  }

  def writeRDD(
                session: SparkSession,
                params: MergedParameters,
                rdd: RDD[String],
                mapper: Option[Map[String, String]] = None,
                jdbcWrapper: JDBCWrapper = DefaultJDBCWrapper,
                source: SupportedSource = SupportedSource.S3INTERNAL
              ): Boolean =
  {
    false
  }

}
