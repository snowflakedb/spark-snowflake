/*
 * Copyright 2015-2019 Snowflake Computing
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

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.io.ParquetUtils
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder.RecordBuilder
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.io.{ByteArrayOutputStream, File, FileOutputStream, FileWriter}
import java.sql.{Date, Timestamp}
import scala.util.Random

// scalastyle:off println
class CloudStorageSuite extends IntegrationSuiteBase {

  import testImplicits._

  private val test_table1: String = s"test_temp_table_$randomSuffix"
  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val test_table_write: String = s"test_table_write_$randomSuffix"
  private lazy val localDF: DataFrame = Seq((1000, "str11"), (2000, "str22")).toDF("c1", "c2")

  private val largeStringValue =
    s"""spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |""".stripMargin.filter(_ >= ' ')
  private val LARGE_TABLE_ROW_COUNT = 900000
  lazy val setupLargeResultTable = {
    jdbcUpdate(
      s"""create or replace table $test_table_large_result (
         | int_c int, c_string string(1024) )""".stripMargin)

    jdbcUpdate(
      s"""insert into $test_table_large_result select
         | row_number() over (order by seq4()) - 1, '$largeStringValue'
         | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))""".stripMargin)
    true
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table1")
      jdbcUpdate(s"drop table if exists $test_table_large_result")
      jdbcUpdate(s"drop table if exists $test_table_write")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $test_table1(c1 int, c2 string)")
    jdbcUpdate(s"insert into $test_table1 values (100, 'str1'),(200, 'str2')")
  }

  private def getHashAgg(tableName: String): java.math.BigDecimal =
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select HASH_AGG(*) from $tableName")
      .load()
      .collect()(0).getDecimal(0)

  private def getRowCount(tableName: String): Long =
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName)
      .load()
      .count()

  // manual test for the proxy protocol
  ignore ("snow-1359291") {
    val spark = sparkSession
    import spark.implicits._
    val df = Seq(1, 2, 3, 4).toDF()
    df.show()
    val proxyConfig = Map[String, String](
      "use_proxy" -> "true",
      "proxy_host" -> "127.0.0.1",
      "proxy_port" -> "8080",
      "proxy_protocol" -> "http"
    )
    df.write.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .options(proxyConfig)
      .option("dbtable", "test12345")
      .mode(SaveMode.Overwrite)
      .save()
  }

  test("write a small DataFrame to GCS with down-scoped-token") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table1)
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getHashAgg(test_table1) == getHashAgg(test_table_write))
    } else {
      println("skip test for non-GCS platform: " +
        "write a small DataFrame to GCS with down-scoped-token")
    }
  }

  test("write a big DataFrame to GCS with down-scoped-token") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      setupLargeResultTable
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("partition_size_in_mb", 1) // generate multiple partitions
        .option("dbtable", test_table_large_result)
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getHashAgg(test_table_large_result) == getHashAgg(test_table_write))
    } else {
      println("skip test for non-GCS platform: " +
        "write a big DataFrame to GCS with down-scoped-token")
    }
  }

  test("write a empty DataFrame to GCS with down-scoped-token") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", s"select * from $test_table1 where 1 = 2")
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getRowCount(test_table_write) == 0)
    } else {
      println("skip test for non-GCS platform: " +
        "write a empty DataFrame to GCS with down-scoped-token")
    }
  }
  def convertFieldToAvro(data: StructType): Schema = {
    val builder: RecordBuilder[Schema] = SchemaBuilder.record("record").namespace("redundant")
    ParquetUtils.convertStructToAvro(data, builder, "redundant")
  }

  class ByteArrayOutputFile(stream: ByteArrayOutputStream) extends OutputFile {
    private val outputStream = stream

    override def supportsBlockSize(): Boolean = false

    def toByteArray: Array[Byte] = outputStream.toByteArray

    override def create(blockSizeHint: Long): PositionOutputStream = createPositionOutputStream()

    override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = {
      createPositionOutputStream()
    }

    override def defaultBlockSize(): Long = 0

    private def createPositionOutputStream(): PositionOutputStream = {
      var pos = 0
      new PositionOutputStream {
        override def getPos: Long = pos

        override def write(b: Int): Unit = {
          outputStream.write(b)
          pos+=1
        }

        override def flush(): Unit = outputStream.flush()

        override def close(): Unit = outputStream.close()

        override def write(b: Array[Byte], off: Int, len: Int): Unit = {
          outputStream.write(b, off, len)
          pos+=len
        }
      }
    }
    def show(): Unit = {
//      print(outputStream.toString("UTF-8"))
      val file = new FileOutputStream("output.parquet")
      file.write(outputStream.toByteArray)
      file.close()
    }
  }

  test("write dataframe to parquet file in aws") {
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table1)
      .load()

    val schema = convertFieldToAvro(df.schema)
    // create parquet writer
    val out = new ByteArrayOutputFile(new ByteArrayOutputStream())
    val writer = AvroParquetWriter.builder[GenericData.Record](out)
      .withSchema(schema)
      //      .withCompressionCodec(CompressionCodecName.GZIP)
      .build()
    try {
      // generate Avro Record and write
      val record = new GenericData.Record(schema)

      record.put("C1", "1")
      record.put("C2", "2")
      writer.write(record)
      writer.close()
    }
    out.show()
  }
  test("read parquet") {
    // Define the schema

    val arrayColumns = (1 to 80).map(i =>
      StructField(s"arrayField$i", ArrayType(IntegerType), nullable = true))
    val otherColumns = (81 to 550).map(i => StructField(s"field$i", DoubleType, nullable = true))
    val schema = StructType(arrayColumns ++ otherColumns)

    // Generate random data
    val data = (1 to 10000).map { _ =>
      val arrayValues = (1 to 80).map(_ =>
        (1 to 100).map(_ => Random.nextInt(100)).toArray) // Random arrays of size 100
      val otherValues = (81 to 550).map(_ =>
        Random.nextDouble()) // Random strings of 10 characters
      Row.fromSeq(arrayValues ++ otherValues)
    }
    val rdd = sparkSession.sparkContext.parallelize(data)
    val df = sparkSession.createDataFrame(rdd, schema)
    val start = System.currentTimeMillis()
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", "test_parquet")
//      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "false")
      .mode(SaveMode.Overwrite)
      .save()
    val end = System.currentTimeMillis()
    print(end - start)

  }
  test("test parquet") {
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table1)
      .load()
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_write)
//      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "false")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
// scalastyle:on println
