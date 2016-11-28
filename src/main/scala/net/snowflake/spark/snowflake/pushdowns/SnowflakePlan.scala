package net.snowflake.spark.snowflake.pushdowns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by ema on 11/22/16.
  */
case class SnowflakePlan(output: Seq[Attribute],
                         rdd: RDD[InternalRow]) extends SparkPlan
{

  override def children: Seq[SparkPlan] = Nil

  protected override def doExecute(): RDD[InternalRow] = {
    rdd
  }
}

object SnowflakePlan {
  def buildRDDFromQuery(sparkSession: SparkSession, tree: AbstractQuery): SnowflakePlan = {

    val treeOutput = tree.qualifiedOutput
    val query = new SQLBuilder()
      .raw("SELECT ")
      .addExpressions(treeOutput, ", ")
      .raw(" FROM ")
      .appendBuilder(tree.collapse)

    val sql = query.sql.toString()
    val sqlParams = query.params

    val baseQueries = tree.findAll { case q: BaseQuery => q }.orNull

    if (baseQueries == null) {
      throw new Exception("Query tree does not terminate with a valid BaseQuery instance.")
    }

    val rdd = baseQueries.head.relation.buildScanFromSQL[InternalRow](sql)

    SnowflakePlan(treeOutput.map(_.toAttribute), rdd)
  }
}
