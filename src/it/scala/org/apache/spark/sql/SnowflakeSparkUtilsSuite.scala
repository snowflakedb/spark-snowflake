package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.snowflake.{SFQueryTest, SFTestSessionBase, SnowflakeSparkUtils}
import org.apache.spark.sql.types.{LongType, StructType}

class SnowflakeSparkUtilsSuite extends SFQueryTest with SFTestSessionBase {

  import SFTestImplicits._

  protected lazy val sql = spark.sql _

  test("unit test: SnowflakeSparkUtils.getJDBCProviderName") {
    assert(SnowflakeSparkUtils.getJDBCProviderName(
      "jdbc:postgresql://localhost/mrui?user=mrui&password=password").equals("postgresql"))
    assert(SnowflakeSparkUtils.getJDBCProviderName(
      "jdbc:mysql://HOST/DATABASE").equals("mysql"))
    assert(SnowflakeSparkUtils.getJDBCProviderName(
      "jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE").equals("microsoft"))
    assert(SnowflakeSparkUtils.getJDBCProviderName(
      "jdbc:as400://HOST/DATABASE").equals("as400")) // db2

    // negative test
    assert(SnowflakeSparkUtils.getJDBCProviderName("").equals("unknown"))
    assert(SnowflakeSparkUtils.getJDBCProviderName("jdbc").equals("unknown"))
    assert(SnowflakeSparkUtils.getJDBCProviderName("jdbc:").equals("unknown"))
    assert(SnowflakeSparkUtils.getJDBCProviderName("jdbc::").equals("unknown"))
    assert(SnowflakeSparkUtils.getJDBCProviderName("jdbc:::").equals("unknown"))
    assert(SnowflakeSparkUtils.getJDBCProviderName("jdbc:provider:").equals("unknown"))
  }

  test("unit test: SnowflakeSparkUtils.getNameForLogicalPlanOrExpression for JDBCRelation") {
    assert(SnowflakeSparkUtils.getNameForLogicalPlanOrExpression(null).equals("NULL"))
    assert(SnowflakeSparkUtils.getNameForLogicalPlanOrExpression("s").equals("java.lang.String"))

    val schema = new StructType().add("a", LongType)
    // case class JDBCRelation(
    //    override val schema: StructType,
    //    parts: Array[Partition],
    //    jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
    val jdbcOptions1 = new JDBCOptions(
      "jdbc:postgresql://localhost/mrui?user=mrui&password=password", "t1", Map.empty)
    val jdbcRelation1 = JDBCRelation(schema, Array.empty, jdbcOptions1)(spark)
    // case class LogicalRelation(
    //    relation: BaseRelation,
    //    output: Seq[AttributeReference],
    //    catalogTable: Option[CatalogTable],
    //    override val isStreaming: Boolean)
    val logicalRelation1 = LogicalRelation(jdbcRelation1)
    assert(SnowflakeSparkUtils.getNameForLogicalPlanOrExpression(logicalRelation1)
      .equals("LogicalRelation:JDBCRelation:postgresql"))

    // case class SaveIntoDataSourceCommand(
    //    query: LogicalPlan,
    //    dataSource: CreatableRelationProvider,
    //    options: Map[String, String],
    //    mode: SaveMode)
    val options = Map("url" -> "jdbc:postgresql://localhost/mrui?user=mrui&password=password")
    val saveIntoDataSourceCommand1 =
      SaveIntoDataSourceCommand(null, new JdbcRelationProvider, options, SaveMode.Append)
    assert(SnowflakeSparkUtils.getNameForLogicalPlanOrExpression(saveIntoDataSourceCommand1)
      .equals("SaveIntoDataSourceCommand:JdbcRelationProvider:postgresql"))
  }

  override protected def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] = Seq.empty
}
