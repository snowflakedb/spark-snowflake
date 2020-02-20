package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{
  TruncTimestamp,
  Attribute,
  Expression
}

import org.apache.spark.sql.catalyst.expressions.TruncTimestamp._

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object DateTruncate {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case TruncTimestamp(dateType, timeStamp,zid) =>
        ConstantString("date_trunc(") + convertStatement(dateType, fields) + "," +
          convertStatement(timeStamp, fields) + ")"

    })
  }
}
