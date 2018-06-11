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
package net.snowflake.spark.snowflake.io

import net.snowflake.spark.snowflake.{CloudCredentialsUtils, JDBCWrapper, Utils}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.{Logger, LoggerFactory}


private[io] class ExternalStageReader(
                  val sqlContext: SQLContext,
                  val params: MergedParameters,
                  val sql: String,
                  val jdbcWrapper: JDBCWrapper,
                  val format: SupportedFormat = SupportedFormat.CSV
                ) extends DataUnloader {
  override val log: Logger = LoggerFactory.getLogger(getClass)


  def getRDD(): RDD[String] = {
    val tempDir = params.createPerQueryTempDir()

    val numRows = setup(
      sql = buildUnloadStmt(
        query = sql,
        location = Utils.fixUrlForCopyCommand(tempDir),
        compression = if (params.sfCompress) "gzip" else "none",
        credentialsString = Some(
          CloudCredentialsUtils.getSnowflakeCredentialsString(sqlContext,
            params))),

      conn = jdbcWrapper.getConnector(params))

    if (numRows == 0) {
      // For no records, create an empty RDD
      sqlContext.sparkContext.emptyRDD[String]
    } else {
      format match {
        case SupportedFormat.CSV =>
          sqlContext.sparkContext.newAPIHadoopFile(
            tempDir,
            classOf[SFCSVInputFormat],
            classOf[java.lang.Long],
            classOf[String]
          ).map(_._2)
        case SupportedFormat.JSON =>
          throw new UnsupportedOperationException("Not support JSON in current version")
          //todo
          sqlContext.sparkContext.emptyRDD[String]
      }
    }
  }

}

