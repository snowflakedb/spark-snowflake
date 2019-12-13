package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.Random

class TruncateTableSuite extends IntegrationSuiteBase {
  val table = s"test_table_$randomSuffix"

  lazy val st1 = new StructType(
    Array(
      StructField("num1", LongType, nullable = false),
      StructField("num2", FloatType, nullable = false)
    )
  )

  lazy val df1: DataFrame = sqlContext.createDataFrame(
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

  lazy val df2: DataFrame = sqlContext.createDataFrame(
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

    // create table
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
    jdbcUpdate(s"drop table if exists $table")
    super.afterAll()
  }
}
