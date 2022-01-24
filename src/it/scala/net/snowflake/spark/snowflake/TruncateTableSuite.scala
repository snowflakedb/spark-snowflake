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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.Random
// scalastyle:off println

class TruncateTableSuite extends IntegrationSuiteBase {
  import testImplicits._

  val normalTable = s"test_table_$randomSuffix"
  val specialTable = s""""test_table_.'!@#$$%^&*"" $randomSuffix""""""
  val targetTable = s""""test_table_target_$randomSuffix""""""

  // This test will test normal table and table name including special characters
  val tableNames = Array(normalTable, specialTable)

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

    tableNames.foreach(table => {
      println(s"""Test table: "$table"""")
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
      assert(checkSchema1(table))
    })
  }

  test("use truncate table without staging table") {

    tableNames.foreach(table => {
      println(s"""Test table: "$table"""")
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

      assert(checkSchema2(table))

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

      assert(checkSchema2(table))

      // replace previous table and overwrite schema
      df1.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", table)
        .option("truncate_table", "off")
        .option("usestagingtable", "off")
        .mode(SaveMode.Overwrite)
        .save()

      assert(checkSchema1(table))

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
      assert(checkSchema1(table))
    })
  }

  test("don't truncate table with staging table") {

    tableNames.foreach(table => {
      println(s"""Test table: "$table"""")
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
      assert(checkSchema2(table))
    })
  }

  test("don't truncate table without staging table") {
    tableNames.foreach(table => {
      println(s"""Test table: "$table"""")
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
      assert(checkSchema2(table))
    })
  }

  test("negative test 1: original table doesn't exist, error happen when writing") {
    tableNames.foreach(table => {
      println(s"""Test table: "$table"""")
      // Make sure table doesnt' exist
      jdbcUpdate(s"drop table if exists $table")
      assert(!DefaultJDBCWrapper.tableExists(params, table.toString))

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
        assert(!DefaultJDBCWrapper.tableExists(params, table.toString))
      })

      // Disable test hook in the end
      TestHook.disableTestHook()
    })
  }

  test("negative test 2: original table exists, error happen when writing") {
    tableNames.foreach(table => {
      println(s"""Test table: "$table"""")
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
    })
  }

  def checkSchema2(tableName: String): Boolean = {
    val st = DefaultJDBCWrapper.resolveTable(conn, tableName, params)
    val st1 = new StructType(
      Array(
        StructField("NUM1", DecimalType(38, 0), nullable = false),
        StructField("NUM2", DecimalType(38, 0), nullable = false)
      )
    )
    st.equals(st1)
  }

  // This test case is used to reproduce/test SNOW-222104
  // The reproducing conditions are:
  // 1. Write data frame to a table with OVERWRITE and (usestagingtable=on truncate_table=off (they are default)).
  // 2. table name includes database name and schema name.
  // 3. sfSchema is configured to a different schema
  // 4. The user has privilege to create stage but doesn't have privilege to create table on sfSchema
  //
  // Below is how to create the env to reproduce it and test
  // 1. create a new role TESTROLE_SPARK_2 with ADMIN.
  // 2. ADMIN grants USAGE and CREATE SCHEMA privilege for testdb_spark to TESTROLE_SPARK_2
  // 3. ADMIN GRANT ROLE TESTROLE_SPARK_2 to USER TEST_SPARK;
  // 4. TEST_SPARK logins, switchs to TESTROLE_SPARK_2.
  // 5. create a managed schema: create schema TEST_SCHEMA_NO_CREATE_TABLE with managed access;
  // 6. grant USAGE and CREATE STAGE on TEST_SCHEMA_NO_CREATE_TABLE to TESTROLE_SPARK.
  //
  // TESTROLE_SPARK is the default role for TEST_SPARK.
  // 1. set sfSchema as TEST_SCHEMA_NO_CREATE_TABLE
  // 2. write with OverWrite to table: testdb_spark.spark_test.table_name
  //
  // NOTE:
  // 1. The test env is only setup for sfctest0 on AWS. So this test only run on AWS.
  // 2. Configure truncate_table = on and usestagingtable=off can workaround this issue.
  test("write table with different schema") {
    val accountName = System.getenv(SNOWFLAKE_TEST_ACCOUNT)
    val isAWS = accountName == null || accountName.equals("aws")
    // It is necessary to check extraTestForCoverage because the released SC back-compatibility
    // regress test for preprod3/QA is on AWS too, but the test schema is not set up there.
    if (isAWS && extraTestForCoverage) {
      tableNames.foreach(table => {
        println(s"""Test table: "$table"""")
        jdbcUpdate(s"drop table if exists $table")

        // Use a different schema, current user has no permission to CREATE TABLE
        // but has permission to CREATE STAGE.
        val sfOptions = replaceOption(connectorOptionsNoTable, "sfschema", "TEST_SCHEMA_NO_CREATE_TABLE")

        val tableFullName = s"testdb_spark.spark_test.$table"
        // create one table
        df2.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(sfOptions)
          .option("dbtable", tableFullName)
          .option("truncate_table", "off")
          .option("usestagingtable", "on")
          .mode(SaveMode.Overwrite)
          .save()

        // check schema
        assert(checkSchema2(tableFullName))
      })
    }
  }

  test("Write empty DataFrame and target table doesn't exist: SNOW-297134") {
    import testImplicits._
    val emptyDf = Seq.empty[(Int, String)].toDF("key", "value")
    // Below CSV and PARQUET WRITE generate empty file.
    // So snowflake should also create an empty table.
    // emptyDf.write.csv("/tmp/output/csv")
    // emptyDf.write.parquet("/tmp/output/parquet")
    // create a table has 3 columns.

    // Make sure target table doesn't exist
    jdbcUpdate(s"drop table if exists $targetTable")
    // Write empty DataFrame with Overwrite mode
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .mode(SaveMode.Overwrite)
      .save()

    // success reads the target table
    var readDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .load()
    assert(readDF.count() == 0 && readDF.schema.fields.length == 2)

    // Make sure target table doesn't exist
    jdbcUpdate(s"drop table if exists $targetTable")
    // Write empty DataFrame with Append mode
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .mode(SaveMode.Append)
      .save()

    // success reads the target table
    readDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .load()
    assert(readDF.count() == 0 && readDF.schema.fields.length == 2)
  }

  test("Write empty DataFrame and target table doesn't exist: SNOW-495389") {
    import testImplicits._
    val emptyDf = Seq.empty[(Int, String)].toDF("key", "value")
    // Create target table
    jdbcUpdate(s"create or replace table $targetTable (c1 int, c2 string)")
    jdbcUpdate(s"insert into $targetTable values (123, 'abc')")

    // case 1: Write DF with Overwrite, truncate_table = on, and usestagingtable = off
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .option("truncate_table", "on")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    // target table is empty, and table is not recreated.
    assert(sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select c1 from $targetTable") // "c1" is old table column name
      .load().count() == 0)

    // case 2: Write DF with Overwrite, truncate_table = on, and usestagingtable = off
    // But disable the fix
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .option("truncate_table", "on")
      .option("usestagingtable", "off")
      .option(Parameters.PARAM_INTERNAL_USE_COPY_TO_WRITE_EMPTY_DATAFRAME, "false")
      .mode(SaveMode.Overwrite)
      .save()

    // If this fix is disabled, the table will be recreated.
    assert(sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select key from $targetTable") // "key" is old table column name
      .load().count() == 0)

    // case 3: Write DF with Overwrite, truncate_table = off, and usestagingtable = off
    jdbcUpdate(s"create or replace table $targetTable (c1 int, c2 string)")
    jdbcUpdate(s"insert into $targetTable values (123, 'abc')")
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .option("truncate_table", "off")
      .option("usestagingtable", "off")
      .mode(SaveMode.Overwrite)
      .save()

    // target table is empty, and table is recreated.
    assert(sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select key from $targetTable") // "key" is new table column name
      .load().count() == 0)
  }

  test("Write empty DataFrame and target table exists: SNOW-297134") {
    import testImplicits._
    val emptyDf = Seq.empty[(Int, String)].toDF("key", "value")
    // Below CSV and PARQUET WRITE generate empty file.
    // So snowflake should also create an empty table.
    // emptyDf.write.csv("/tmp/output/csv")
    // emptyDf.write.parquet("/tmp/output/parquet")
    // create a table has 3 columns.

    jdbcUpdate(s"create or replace table $targetTable (c1 int, c2 int, c3 int)")
    jdbcUpdate(s"insert into $targetTable values (1, 2 ,3)")
    // Write empty DataFrame with Override mode
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .mode(SaveMode.Overwrite)
      .save()

    // success reads the target table
    var readDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .load()
    // The table has been over written, so it only has 2 columns.
    assert(readDF.schema.fields.length == 2 && readDF.count() == 0)

    jdbcUpdate(s"create or replace table $targetTable (c1 int, c2 int, c3 int)")
    jdbcUpdate(s"insert into $targetTable values (1, 2 ,3)")
    // Write empty DataFrame with Append mode
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .mode(SaveMode.Append)
      .save()

    // success reads the target table
    readDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .load()
    // The table has not been over written, so it has 3 columns
    assert(readDF.schema.fields.length == 3 && readDF.count() == 1)
  }

  test("Negative test to write empty DataFrame: SNOW-297134") {
    import testImplicits._
    val emptyDf = Seq.empty[(Int, String)].toDF("key", "value")

    // Make sure target table doesn't exist
    jdbcUpdate(s"drop table if exists $targetTable")
    // Write empty DataFrame
    emptyDf.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", targetTable)
      .option(Parameters.PARAM_INTERNAL_SKIP_WRITE_WHEN_WRITING_EMPTY_DATAFRAME, "true")
      .mode(SaveMode.Overwrite)
      .save()

    // Fail to read table because no table is created in last step.
    assertThrows[Exception]({
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", targetTable)
        .load()
        .count()
    })
  }

  test("AWS use region url with small data") {
    // This test case only affect AWS account
    val accountName: String = Option(System.getenv(SNOWFLAKE_TEST_ACCOUNT)).getOrElse("aws")
    if (accountName.equalsIgnoreCase("aws")) {
      // Make sure target table doesn't exist
      jdbcUpdate(s"create or replace table $targetTable (c1 int, c2 string)")

      // Write DataFrame and commit
      val df = Seq[(Int, String)]((1, "a"), (2, "b")).toDF("key", "value")
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", targetTable)
        .option(Parameters.PARAM_INTERNAL_USE_AWS_REGION_URL, "false")
        .mode(SaveMode.Append)
        .save()
      assert(getRowCount(targetTable) == 2)

      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", targetTable)
        .option(Parameters.PARAM_INTERNAL_USE_AWS_REGION_URL, "true")
        .mode(SaveMode.Append)
        .save()
      assert(getRowCount(targetTable) == 4)
    }
  }

  test("AWS use region url with large data") {
    // This test case only affect AWS account
    val accountName: String = Option(System.getenv(SNOWFLAKE_TEST_ACCOUNT)).getOrElse("aws")
    if (accountName.equalsIgnoreCase("aws")) {
      def getRandomString(len: Int): String = {
        Random.alphanumeric take len mkString ""
      }

      val partitionCount = 4
      val rowCountPerPartition = 1024
      val strValue = getRandomString(512)
      // Create RDD which generates 1 large partition
      val testRDD: RDD[Row] = sparkSession.sparkContext
        .parallelize(Seq[Int](), partitionCount)
        .mapPartitions { _ => {
          (1 to rowCountPerPartition).map { _ => {
            Row(strValue, strValue, strValue, strValue)
          }
          }.iterator
        }
        }
      val schema = StructType(
        List(
          StructField("str1", StringType),
          StructField("str2", StringType),
          StructField("str3", StringType),
          StructField("str4", StringType)
        )
      )

      // Write to snowflake
      sparkSession.createDataFrame(testRDD, schema).write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", targetTable)
        .option(Parameters.PARAM_INTERNAL_USE_AWS_REGION_URL, "false")
        .mode(SaveMode.Overwrite)
        .save()
      assert(getRowCount(targetTable) == partitionCount * rowCountPerPartition)

      // Write to the table again
      sparkSession.createDataFrame(testRDD, schema).write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", targetTable)
        .option(Parameters.PARAM_INTERNAL_USE_AWS_REGION_URL, "true")
        .mode(SaveMode.Append)
        .save()
      assert(getRowCount(targetTable) == partitionCount * rowCountPerPartition * 2)
    }
  }

  def checkSchema1(tableName: String): Boolean = {
    val st = DefaultJDBCWrapper.resolveTable(conn, tableName, params)
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
    jdbcUpdate(s"drop table if exists $normalTable")
    jdbcUpdate(s"drop table if exists $specialTable")
    jdbcUpdate(s"drop table if exists $targetTable")
    super.afterAll()
  }
}

// scalastyle:on println
