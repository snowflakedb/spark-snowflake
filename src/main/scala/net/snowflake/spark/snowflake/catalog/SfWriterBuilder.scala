package net.snowflake.spark.snowflake.catalog

import net.snowflake.spark.snowflake.JDBCWrapper
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.sql._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.InsertableRelation

case class SfWriterBuilder(jdbcWrapper: JDBCWrapper, params: MergedParameters) extends WriteBuilder
  with SupportsTruncate {
  private var isTruncate = false

  override def truncate(): WriteBuilder = {
    isTruncate = true
    this
  }

  override def build(): V1Write = new V1Write {
    override def toInsertableRelation: InsertableRelation = (data: DataFrame, _: Boolean) => {
      val saveMode = if (isTruncate) {
        SaveMode.Overwrite
      } else {
        SaveMode.Append
      }
      val writer = new net.snowflake.spark.snowflake.SnowflakeWriter(jdbcWrapper)
      writer.save(data.sqlContext, data, saveMode, params)
    }
  }
}