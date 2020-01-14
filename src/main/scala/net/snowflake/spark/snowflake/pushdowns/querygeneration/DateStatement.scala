package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  DateAdd,
  Expression,
  ToDate,
  Month,
  Quarter,
  Year
}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object DateStatement {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case DateAdd(startDate, days) =>
        ConstantString("DATEADD(day,") + convertStatement(days, fields) + "," +
          convertStatement(startDate, fields) + ")"

      case _: Month | _: Quarter | _: Year | _: ToDate =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case _ => null
    })
  }
}
