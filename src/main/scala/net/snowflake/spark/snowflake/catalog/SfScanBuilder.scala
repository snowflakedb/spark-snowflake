package net.snowflake.spark.snowflake.catalog

import net.snowflake.spark.snowflake.{FilterPushdown, JDBCWrapper, SnowflakeRelation}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class SfScanBuilder(session: SparkSession,
                          schema: StructType,
                          params: MergedParameters,
                         jdbcWrapper: JDBCWrapper)   extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging{
  private val isCaseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private var pushedFilter = Array.empty[Filter]

  private var finalSchema = schema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, unSupported) = filters.partition(
      filter =>
        FilterPushdown
          .buildFilterStatement(
            schema,
            filter,
            true
          )
          .isDefined
    )
    this.pushedFilter = pushed
    unSupported
  }

  override def pushedFilters(): Array[Filter] = pushedFilter

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive))
      .toSet
    val fields = schema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredCols.contains(colName)
    }
    finalSchema = StructType(fields)
  }

  override def build(): Scan = {
    SfScan(SnowflakeRelation(jdbcWrapper, params,
      Option(schema))(session.sqlContext), finalSchema, pushedFilters)
  }
}
