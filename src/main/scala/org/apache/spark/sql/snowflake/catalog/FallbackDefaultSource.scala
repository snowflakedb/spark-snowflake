/*
 * Copyright 2015-2025 Snowflake Computing
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
package org.apache.spark.sql.snowflake.catalog

import net.snowflake.spark.snowflake.{DefaultSource, Parameters, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

/**
 * Provider of the V1 tables that SnowflakeFallbackCatalog returns after a 403. Applies Iceberg
 * time travel read options (IcebergTimeTravel) before the regular Snowflake source runs, and
 * rejects branch writes; the Snowflake source itself is unchanged.
 */
class FallbackDefaultSource extends DefaultSource {

  override def createRelation(
      sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val params = readParameters(sqlContext, parameters)
    FallbackRelation(super.createRelation(sqlContext, params), params)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val params = readParameters(sqlContext, parameters)
    FallbackRelation(super.createRelation(sqlContext, params, schema), params)
  }

  /**
   * Time travel read options, then spark.wap.branch: as in Iceberg, a read that names no
   * version reads the WAP branch once the branch exists. Iceberg also requires the table
   * property write.wap.enabled, which a 403 hides; setting spark.wap.branch stands for it.
   */
  private def readParameters(
      sqlContext: SQLContext, parameters: Map[String, String]): Map[String, String] = {
    val params = IcebergTimeTravel.rewriteReadParameters(parameters)
    (IcebergTimeTravel.wapBranch(sqlContext.sparkSession.conf),
      params.get(Parameters.PARAM_SF_DBTABLE)) match {
      case (Some(branch), Some(dbtable))
          if !params.contains(Parameters.PARAM_SF_QUERY) && branchExists(params, dbtable, branch) =>
        params - Parameters.PARAM_SF_DBTABLE + (Parameters.PARAM_SF_QUERY ->
          IcebergTimeTravel.query(dbtable, IcebergTimeTravel.Branch(branch)))
      case _ => params
    }
  }

  protected def branchExists(
      params: Map[String, String], dbtable: String, branch: String): Boolean =
    Utils.runQuery(params, IcebergTimeTravel.branchExistsQuery(dbtable, branch)).next()
}

private[catalog] object FallbackRelation {
  type SnowflakeV1Relation = BaseRelation with PrunedFilteredScan with InsertableRelation

  def apply(relation: BaseRelation, params: Map[String, String]): FallbackRelation =
    relation match {
      case r: BaseRelation with PrunedFilteredScan with InsertableRelation =>
        new FallbackRelation(r, timeTravel = params.contains(Parameters.PARAM_SF_QUERY))
      case other =>
        throw new IllegalStateException(s"Unexpected Snowflake relation ${other.getClass.getName}")
    }
}

/** The Snowflake relation of a fallback table; only insert adds the branch write check. */
private[catalog] class FallbackRelation(
    val relation: FallbackRelation.SnowflakeV1Relation, val timeTravel: Boolean)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation {

  override def sqlContext: SQLContext = relation.sqlContext
  override def schema: StructType = relation.schema
  override def sizeInBytes: Long = relation.sizeInBytes
  override def needConversion: Boolean = relation.needConversion
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] =
    relation.unhandledFilters(filters)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    relation.buildScan(requiredColumns, filters)

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val conf = sqlContext.sparkSession.conf
    IcebergTimeTravel.checkWrite(
      timeTravel, conf.getOption("spark.wap.branch"), conf.getOption("spark.wap.id"))
    relation.insert(data, overwrite)
  }

  override def toString: String = relation.toString
}
