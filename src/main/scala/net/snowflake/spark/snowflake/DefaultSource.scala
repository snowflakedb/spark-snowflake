/*
 * Copyright 2015-2018 Snowflake Computing
 * Copyright 2015 TouchType Ltd
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
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

/**
  * Snowflake Source implementation for Spark SQL
  * Major TODO points:
  *   - Add support for compression Snowflake->Spark
  *   - Add support for using Snowflake Stage files, so the user doesn't need
  * to provide AWS passwords
  *   - Add support for VARIANT
  */
class DefaultSource(jdbcWrapper: JDBCWrapper)
  extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  private val log = LoggerFactory.getLogger(getClass)

  /**
    * Default constructor required by Data Source API
    */
  def this() = this(DefaultJDBCWrapper)

  /**
    * Create a new `SnowflakeRelation` instance using parameters from Spark SQL DDL. Resolves the schema
    * using JDBC connection over provided URL, which must contain credentials.
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    //check spark version for push down
    if (params.autoPushdown)
      SnowflakeConnectorUtils.checkVersionAndEnablePushdown(sqlContext.sparkSession)
    SnowflakeRelation(jdbcWrapper, params, None)(sqlContext)
  }

  /**
    * Load a `SnowflakeRelation` using user-provided schema, so no inference over JDBC will be used.
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    //check spark version for push down
    if (params.autoPushdown)
      SnowflakeConnectorUtils.checkVersionAndEnablePushdown(sqlContext.sparkSession)
    SnowflakeRelation(jdbcWrapper, params, Some(schema))(sqlContext)
  }

  /**
    * Creates a Relation instance by first writing the contents of the given DataFrame to Snowflake
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               saveMode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {

    val params = Parameters.mergeParameters(parameters)
    //check spark version for push down
    if (params.autoPushdown)
      SnowflakeConnectorUtils.checkVersionAndEnablePushdown(sqlContext.sparkSession)
    val table = params.table.getOrElse {
      throw new IllegalArgumentException(
        "For save operations you must specify a Snowfake table name with the 'dbtable' parameter")
    }

    def tableExists: Boolean = {
      val conn = jdbcWrapper.getConnector(params)
      try {
        jdbcWrapper.tableExists(conn, table.toString)
      } finally {
        conn.close()
      }
    }

    val (doSave, dropExisting) = saveMode match {
      case SaveMode.Append => (true, false)
      case SaveMode.Overwrite => (true, true)
      case SaveMode.ErrorIfExists =>
        if (tableExists) {
          sys.error(s"Table $table already exists! (SaveMode is set to ErrorIfExists)")
        } else {
          (true, false)
        }
      case SaveMode.Ignore =>
        if (tableExists) {
          log.info(s"Table $table already exists -- ignoring save request.")
          (false, false)
        } else {
          (true, false)
        }
    }

    if (doSave) {
      val updatedParams = parameters.updated("overwrite", dropExisting.toString)
      new SnowflakeWriter(jdbcWrapper)
        .save(sqlContext, data, saveMode, Parameters.mergeParameters(updatedParams))

    }

    createRelation(sqlContext, parameters)
  }
}
