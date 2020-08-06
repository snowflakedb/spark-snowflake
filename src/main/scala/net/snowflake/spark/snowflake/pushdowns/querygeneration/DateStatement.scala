package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/** Extractor for Date and Timestamp expressions. */
private[querygeneration] object DateStatement {
  // DateAdd's pretty name in Spark is "date_add",
  // the counterpart's name in SF is "DATEADD".
  // And the syntax is some different.
  val SNOWFLAKE_DATEADD = "DATEADD"

  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case DateAdd(startDate, days) =>
        dateAddStatement(
          datePart = "DAY",
          isSubtract = false,
          convertStatement(days, fields).toString,
          convertStatement(startDate, fields)
        )

      // Snowflake has no direct DateSub function,
      // it is pushdown by DATEADD with negative days
      case DateSub(startDate, days) =>
        dateAddStatement(
          datePart = "DAY",
          isSubtract = true,
          convertStatement(days, fields).toString,
          convertStatement(startDate, fields)
        )

      case AddMonths(startDate, days) =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatement(startDate, fields) + "," +
            convertStatement(days, fields))

      case _: Month | _: Quarter | _: Year |
           _: TruncDate | _: TruncTimestamp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case UnixTimestamp(timeExp, format, _) =>
        // There is no direct equivalent function for Spark unix_timestamp()
        // in Snowflake. But, the equivalent functionality can be achieved by
        // DATE_PART( <date_or_time_part> , <date_or_time_expr> ).
        // Some highlights to use DATE_PART():
        // 1. Spark unix_timestamp() is used to convert timestamp in seconds.
        //    So Snowflake needs to use 'EPOCH_SECOND' for <date_or_time_part>.
        // 2. Spark unix_timestamp() supports column type: Date, Timestamp
        //    and String. Snowflake DATE_PART() supports the column types:
        //    Date and Timestamp.
        //    a) If datatype is DateType or TimestampType, we can use
        //       "DATE_PART('EPOCH_SECOND', <Col>)" directly.
        //    b) If datatype is String, Spark requires customer to provide
        //       format (the default format is "yyyy-MM-dd HH:mm:ss").
        //       So, we can use DATE_PART() + TO_TIMESTAMP(). For example,
        //       "DATE_PART('EPOCH_SECOND', TO_TIMESTAMP(<Col>, format))"
        // 3. Spark also supports unix_timestamp() to get current timestamp.
        //    For this case, Spark has calculated it as a CONST value.
        //    Pushdown doesn't need to care about it.
        timeExp.dataType match {
          case _: DateType | _: TimestampType =>
            ConstantString("DATE_PART('EPOCH_SECOND',") +
              blockStatement(convertStatement(timeExp, fields)) + ")"

          case StringType =>
            // Spark uses Java SimpleDateFormat pattern for 'format'
            // which is different to Snowflake's timestamp format.
            // So, it needs to be converted before being passed to snowflake.
            // If the format are not supported by Snowflake, it is not
            // pushdown to snowflake.
            // Note: the format validity check is basic, It is possible that
            //       some unsupported format is passed. But it doesn't matter
            //       because snowflake will raise error for unsupported format.
            //       Spark will retry without the pushdown to make sure the
            //       final result to be correct.
            getSnowflakeTimestampFormat(format.toString()) match {
              case Some(sfFormat) =>
                ConstantString("DATE_PART('EPOCH_SECOND', TO_TIMESTAMP") +
                  blockStatement(convertStatement(timeExp, fields) +
                    s",'$sfFormat'") + ")"
              case _ =>
                // If the format is not supported, it is not pushdown.
                null
            }
        }

      // Snowflake has no direct TimeSub function,
      // it is pushdown by DATEADD
      case TimeSub(timeExp, format, _) =>
        format match {
          case Literal(value: CalendarInterval, _: CalendarIntervalType) =>
            generateDateAddStatement(
              isSubtract = true,
              value,
              convertStatement(timeExp, fields)
            )
        }

      // Snowflake has no direct TimeAdd function,
      // it is pushdown by DATEADD
      case TimeAdd(timeExp, format, _) =>
        format match {
          case Literal(value: CalendarInterval, _: CalendarIntervalType) =>
            generateDateAddStatement(
              isSubtract = false,
              value,
              convertStatement(timeExp, fields)
            )
        }

      case _ => null
    })
  }

  // Generate DateAdd statement
  private def dateAddStatement(datePart: String,
                               isSubtract: Boolean,
                               value: String,
                               childStmt: SnowflakeSQLStatement)
  : SnowflakeSQLStatement = {
    val adjustValue = if (isSubtract) {
      s"(0 - ($value))"
    } else {
      value
    }
    ConstantString(SNOWFLAKE_DATEADD) +
      blockStatement(
        ConstantString(s"'$datePart', $adjustValue, ") + childStmt
      )
  }

  // Generate DateAdd statement for CalendarInterval.
  // Note: There are 3 fields in CalendarInterval (ms, days, months).
  //       Below 3 cases are valid:
  //       1) each field can be positive/negative/zero.
  //       2) All fields can be 0,
  //       3) more than one fields are non-zero.
  private def generateDateAddStatement(isSubtract: Boolean,
                                       interval: CalendarInterval,
                                       childStmt: SnowflakeSQLStatement)
  : SnowflakeSQLStatement = {
    var resultStmt = childStmt
    if (interval.microseconds != 0L) {
      resultStmt = dateAddStatement(
        "MICROSECOND", isSubtract, interval.microseconds.toString, resultStmt)
    }
    if (interval.days != 0L) {
      resultStmt = dateAddStatement(
        "DAY", isSubtract, interval.days.toString, resultStmt)
    }
    if (interval.months != 0L) {
      resultStmt = dateAddStatement(
        "MONTH", isSubtract, interval.months.toString, resultStmt)
    }
    resultStmt
  }

  // Spark uses Java SimpleDateFormat pattern for 'format'.
  // So it needs to be converted before being passed to snowflake.
  // The format can be converted to Snowflake format if it only includes
  // below parts:
  // 1. Year:               valid value is: "yy" or "yyyy"
  // 2. month in year:      valid value is: "MM"
  // 3. day in month:       valid value is: "dd"
  // 4. hour in day:        valid value is: "HH"
  // 5. minute in hour:     valid value is: "mm"
  // 6. second in minute:   valid value is: "ss"
  // 7. millisecond:        valid value is: "SSS"
  // 8. Literal:  quoted sting or non-special letters
  // Note: This is a basic check. It is possible that some
  //       unsupported format are passed. But it doesn't matter
  //       snowflake will raise error for unsupported format.
  //       Spark will retry without the pushdown to make sure the
  //       final result to be correct.
  private[querygeneration] def getSnowflakeTimestampFormat(format: String)
  : Option[String] = {
    // Below page includes the list of symbols used for Java Timestamp format.
    // The format is regarded as supported if there is no unsupported chars.
    // https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
    val unsupportedChars = Set(
      'G', // era designator
      'h', // hour in am/pm (1-12)
      'E', // day in week
      'D', // day in year
      'F', // day of week in month
      'w', // week in year
      'W', // week in month
      'a', // am/pm marker
      'k', // hour in day (1-24)
      'K', // hour in am/pm (0-11)
      'z'  // time zone
    )

    var inLiteral = false
    var foundUnsupportedChar = false
    // Determine whether there is unsupported format.
    format.foreach(c => {
      if (c == '\'') {
        inLiteral = !inLiteral
      } else if (!inLiteral) {
        if (unsupportedChars.contains(c)) {
          foundUnsupportedChar = true
        }
      } else {
        // Skip the check for quoted literal.
      }
    })

    if (foundUnsupportedChar || inLiteral) {
      log.info(s"DateStatement: unsupported format '$format'")
      None
    } else {
      // The format is supported, convert it to snowflake format by
      // replacing 3 patterns:
      // 1. Java uses 'mm' stand for minutes, but Snowflake uses 'MI',
      // 2. Java use "SSS" for millisecond, but Snowflake uses "FF3"
      // 3. Java uses single quote for Literal string,
      //    but Snowflake uses double quote
      Some(format
        .replaceAll("mm", "MI")
        .replaceAll("SSS", "FF3")
        .replaceAll("'", "\"")
      )
    }
  }
}
