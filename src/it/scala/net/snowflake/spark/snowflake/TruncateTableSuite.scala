package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.SnowflakeSQLException
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import net.snowflake.spark.snowflake.test.TestHookFlag.{
  TH_WRITE_ERROR_AFTER_COPY_INTO,
  TH_WRITE_ERROR_AFTER_CREATE_NEW_TABLE,
  TH_WRITE_ERROR_AFTER_DROP_OLD_TABLE,
  TH_WRITE_ERROR_AFTER_TRUNCATE_TABLE
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.Random
// scalastyle:off println

class TruncateTableSuite extends IntegrationSuiteBase {
  val table = s"test_table_$randomSuffix"

  lazy val st1 = new StructType(
    Array(
      StructField("num1", LongType, nullable = false),
      StructField("num2", FloatType, nullable = false)
    )
  )

  lazy val df1: DataFrame = sparkSession.createDataFrame(
    sc.parallelize(1 to 100)
      .map[Row](_ => {
        val rand = new Random(System.nanoTime())
        Row(rand.nextLong(), rand.nextFloat())
      }),
    st1
  )

  lazy val st2 = new StructType(
    Array(
      StructField("num1", IntegerType, nullable = false),
      StructField("num2", IntegerType, nullable = false)
    )
  )

  lazy val df2: DataFrame = sparkSession.createDataFrame(
    sc.parallelize(1 to 100)
      .map[Row](_ => {
        val rand = new Random(System.nanoTime())
        Row(rand.nextInt(), rand.nextInt())
      }),
    st2
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("use truncate table with staging table") {

    jdbcUpdate(s"drop table if exists $table")

    // create one table
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // replace previous table and overwrite schema
    df1.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // truncate previous table and keep schema
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "on")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // check schema
    assert(checkSchema1())

  }

  test("use truncate table without staging table") {

    jdbcUpdate(s"drop table if exists $table")

    // create table with Append mode
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "on")
      .option("usestagingtable", "off")
      .mode(SaveMode.Append)
      .save()

    assert(checkSchema2())

    jdbcUpdate(s"drop table if exists $table")

    // create table with Overwrite mode
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "on")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    assert(checkSchema2())

    // replace previous table and overwrite schema
    df1.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    assert(checkSchema1())

    // truncate table and keep schema
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "on")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    // checker schema
    assert(checkSchema1())

  }

  test("don't truncate table with staging table") {

    jdbcUpdate(s"drop table if exists $table")

    // create one table
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // replace previous table and overwrite schema
    df1.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // truncate previous table and overwrite schema
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // check schema
    assert(checkSchema2())

  }
  test("don't truncate table without staging table") {

    jdbcUpdate(s"drop table if exists $table")

    // create one table
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    // replace previous table and overwrite schema
    df1.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    // truncate previous table and overwrite schema
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    // check schema
    assert(checkSchema2())
  }

  test("negative test 1: original table doesn't exist, error happen when writing") {
    // Make sure table doesnt' exist
    jdbcUpdate(s"drop table if exists $table")
    assert(!DefaultJDBCWrapper.tableExists(conn, table.toString))

    // Old table doesn't exist so DROP table and TRUNCATE table never happen
    val testConditions = Array(
      (TH_WRITE_ERROR_AFTER_CREATE_NEW_TABLE, df2, table, "on", "off", SaveMode.Append),
      (TH_WRITE_ERROR_AFTER_COPY_INTO, df2, table, "on", "off", SaveMode.Overwrite)
    )

    testConditions.map(x => {
      println(s"Test case 1 condition: $x")

      val testFlag = x._1
      val df = x._2
      val tableName = x._3
      val truncate_table = x._4
      val usestagingtable = x._5
      val saveMode = x._6
      assertThrows[SnowflakeSQLException] {
        TestHook.enableTestFlagOnly(testFlag)
          df.write
            .format(SNOWFLAKE_SOURCE_NAME)
            .options(connectorOptionsNoTable)
            .option("dbtable", tableName)
            .option("truncate_table", truncate_table)
            .option("usestagingtable", usestagingtable)
            .mode(saveMode)
            .save()
      }

      // The original table should not exist
      assert( !DefaultJDBCWrapper.tableExists(conn, table.toString))
    })

    // Disable test hook in the end
    TestHook.disableTestHook()
  }

  test("negative test 2: original table exists, error happen when writing") {
    // Make sure table doesnt' exist
    jdbcUpdate(s"drop table if exists $table")
    // create one table
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("truncate_table", "off")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    val oldRowCount = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$table")
      .load()
      .count()

    // Test only covers truncate_table=on and usestagingtable=off
    val testConditions = Array(
      // In Append mode, failure may happen after COPY_INTO
      (TH_WRITE_ERROR_AFTER_COPY_INTO, df2, table, "on", "off", SaveMode.Append),
      // In Overwrite mode, failure may happen after after truncate table
      (TH_WRITE_ERROR_AFTER_TRUNCATE_TABLE, df2, table, "on", "off", SaveMode.Overwrite),
      // In Overwrite mode, failure may happen after after copy into
      (TH_WRITE_ERROR_AFTER_COPY_INTO, df2, table, "on", "off", SaveMode.Overwrite)
    )

    testConditions.map(x => {
      println(s"Test case 2 condition: $x")

      val testFlag = x._1
      val df = x._2
      val tableName = x._3
      val truncate_table = x._4
      val usestagingtable = x._5
      val saveMode = x._6
      assertThrows[SnowflakeSQLException] {
        TestHook.enableTestFlagOnly(testFlag)
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptionsNoTable)
          .option("dbtable", tableName)
          .option("truncate_table", truncate_table)
          .option("usestagingtable", usestagingtable)
          .mode(saveMode)
          .save()
      }

      val newRowCount = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", s"$table")
        .load()
        .count()
      assert(newRowCount == oldRowCount)
    })

    // Disable test hook in the end
    TestHook.disableTestHook()
  }

  def checkSchema2(): Boolean = {
    val st = DefaultJDBCWrapper.resolveTable(conn, table, params)
    val st1 = new StructType(
      Array(
        StructField("NUM1", DecimalType(38, 0), nullable = false),
        StructField("NUM2", DecimalType(38, 0), nullable = false)
      )
    )
    st.equals(st1)
  }

  def checkSchema1(): Boolean = {
    val st = DefaultJDBCWrapper.resolveTable(conn, table, params)
    val st1 = new StructType(
      Array(
        StructField("NUM1", DecimalType(38, 0), nullable = false),
        StructField("NUM2", DoubleType, nullable = false)
      )
    )
    st.equals(st1)
  }

  override def afterAll(): Unit = {
    TestHook.disableTestHook()
    jdbcUpdate(s"drop table if exists $table")
    super.afterAll()
  }
}

// scalastyle:on println
