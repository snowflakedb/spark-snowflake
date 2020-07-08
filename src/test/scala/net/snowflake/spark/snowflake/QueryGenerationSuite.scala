package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.pushdowns.querygeneration.SourceQuery
import org.scalatest.FunSuite

import scala.collection.immutable.HashMap

class QueryGenerationSuite extends FunSuite{

  test("Test canJoin") {
    val paremeter1 = HashMap(
      (Parameters.PARAM_SF_URL, "url"),
      (Parameters.PARAM_SF_DATABASE, "db"),
      (Parameters.PARAM_SF_WAREHOUSE, "wh"),
      (Parameters.PARAM_SF_DBTABLE, "table"))
    val relation1 = new SnowflakeRelation(null, new MergedParameters(paremeter1), null)(null)
    val sourceQuery1 = SourceQuery(relation1, Seq(), "alias")

    val paremeter2 = HashMap(
      (Parameters.PARAM_SF_URL, "url"),
      (Parameters.PARAM_SF_DATABASE, "db"),
      (Parameters.PARAM_SF_WAREHOUSE, "wh"),
      (Parameters.PARAM_SF_DBTABLE, "table"))
    val relation2 = new SnowflakeRelation(null, new MergedParameters(paremeter2), null)(null)
    val sourceQuery2 = SourceQuery(relation2, Seq(), "alias")

    val paremeter3 = HashMap(
      (Parameters.PARAM_SF_URL, "urllllllll"),
      (Parameters.PARAM_SF_DATABASE, "db"),
      (Parameters.PARAM_SF_WAREHOUSE, "wh"),
      (Parameters.PARAM_SF_DBTABLE, "table"))
    val relation3 = new SnowflakeRelation(null, new MergedParameters(paremeter3), null)(null)
    val sourceQuery3 = SourceQuery(relation3, Seq(), "alias")

    assert(sourceQuery1.canJoin(sourceQuery1))
    assert(sourceQuery1.canJoin(sourceQuery2))
    assert(!sourceQuery1.canJoin(sourceQuery3))
  }

}
