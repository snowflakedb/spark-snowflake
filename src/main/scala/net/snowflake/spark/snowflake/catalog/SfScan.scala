package net.snowflake.spark.snowflake.catalog

import net.snowflake.spark.snowflake.SnowflakeRelation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.V1Scan
import org.apache.spark.sql.sources.{BaseRelation, Filter, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

case class SfScan(
    relation: SnowflakeRelation,
    prunedSchema: StructType,
    pushedFilters: Array[Filter]
) extends V1Scan {

  override def readSchema(): StructType = prunedSchema

  override def toV1TableScan[T <: BaseRelation with TableScan](
      context: SQLContext
  ): T = {
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context
      override def schema: StructType = prunedSchema
      override def needConversion: Boolean = relation.needConversion
      override def buildScan(): RDD[Row] = {
        val columnList = prunedSchema.map(_.name).toArray
        relation.buildScan(columnList, pushedFilters)
      }
    }.asInstanceOf[T]
  }

  override def description(): String = {
    super.description() + ", prunedSchema: " + seqToString(prunedSchema) +
      ", PushedFilters: " + seqToString(pushedFilters)
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")
}
