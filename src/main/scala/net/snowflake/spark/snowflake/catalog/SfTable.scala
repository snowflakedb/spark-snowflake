package net.snowflake.spark.snowflake.catalog

import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.{DefaultJDBCWrapper, JDBCWrapper}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.Connection
import java.util
import scala.collection.JavaConverters._

case class SfTable(ident: Identifier,
                   jdbcWrapper: JDBCWrapper,
                   params: MergedParameters) extends Table
  with SupportsRead
  with SupportsWrite
  with Logging {

  override def name(): String = (ident.namespace() :+ ident.name()).mkString(".")

  override def schema(): StructType = {
    val conn: Connection = DefaultJDBCWrapper.getConnector(params)
    try {
      conn.tableSchema(name, params)
    } finally {
      conn.close()
    }
  }

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ,
      TableCapability.V1_BATCH_WRITE,
      TableCapability.TRUNCATE).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    SfScanBuilder(SparkSession.active, schema, params, jdbcWrapper)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    SfWriterBuilder(jdbcWrapper, params)
  }
}
