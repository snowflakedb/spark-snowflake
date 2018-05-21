package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.SupportedSource.SupportedSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

package object io {

  def readRDD(
               sqlContext: SQLContext,
               params: MergedParameters,
               sql: String,
               jdbcWrapper: JDBCWrapper = DefaultJDBCWrapper,
               source: SupportedSource = SupportedSource.S3INTERNAL,
               format: SupportedFormat = SupportedFormat.CSV
             ): RDD[String] =
    source match {
      case SupportedSource.S3INTERNAL =>
        new S3InternalRDD(sqlContext, params, sql, jdbcWrapper, format)
      case SupportedSource.S3EXTERNAL =>
        new S3External(sqlContext, params, sql, jdbcWrapper, format).getRDD()
    }


  def writeRDD(
                sqlContext: SQLContext,
                params: MergedParameters,
                rdd: RDD[String],
                schema: StructType,
                saveMode: SaveMode,
                mapper: Option[Map[String, String]] = None,
                jdbcWrapper: JDBCWrapper = DefaultJDBCWrapper,
                source: SupportedSource = SupportedSource.S3INTERNAL
              ): Unit =
    source match {
      case SupportedSource.S3INTERNAL =>
        S3Writer.writeToS3Internal(
          rdd,
          schema,
          sqlContext,
          saveMode,
          params,
          jdbcWrapper,
          SupportedSource.S3INTERNAL)
      case SupportedSource.S3EXTERNAL =>
    }

}
