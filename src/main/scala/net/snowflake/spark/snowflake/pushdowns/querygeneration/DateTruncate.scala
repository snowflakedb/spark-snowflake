package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  date_trunc,
  Expression,
  Month,
  Quarter,
  Year,
  Day,
  Minute,
  Second,
}

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object DateTruncate {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    expr  = expr match {
      case _: YYYY | _: YY => Year
      case _: MON | _: MM => Month
      case _: DAY | _: DD => Day
      case _ => expr
    }

    Option(expr match {
      case date_trunc(dateType, timeStamp) =>
        ConstantString("DATE_TRUNC(" + convertStatement(dateType, fields) + "," +
          convertStatement(timeStamp, fields) + ")"

      case _: Month | _: Quarter | _: Year | _: HOUR | _: MINUTE | _: SECOND | _: WEEK =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case _ => null
    })
  }
}
