/*
 * Copyright 2018 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.snowflake.spark.snowflake

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.SupportedSource.SupportedSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Interface to IO component
  */
package object io {

  /**
    * Read a String RDD from Snowflake through given source
    */
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


  /**
    * Write a String RDD to Snowflake through given source
    */
  def writeRDD(
                sqlContext: SQLContext,
                params: MergedParameters,
                rdd: RDD[String],
                schema: StructType,
                saveMode: SaveMode,
                mapper: Option[Map[String, String]] = None,
                jdbcWrapper: JDBCWrapper = DefaultJDBCWrapper,
                source: SupportedSource = SupportedSource.S3INTERNAL,
                s3ClientFactory: Option[AWSCredentials => AmazonS3Client] = None
              ): Unit =
    source match {
      case SupportedSource.S3INTERNAL | SupportedSource.S3EXTERNAL =>
        if(s3ClientFactory.isEmpty) throw new IllegalArgumentException("s3ClientFactory should be provided when using s3 stage")

        S3Writer.writeToS3(
          rdd,
          schema,
          sqlContext,
          saveMode,
          params,
          jdbcWrapper,
          s3ClientFactory.get
        )
      case _ =>
    }

}
