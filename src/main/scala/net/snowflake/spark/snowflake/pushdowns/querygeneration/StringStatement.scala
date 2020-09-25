package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.BinaryType

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object StringStatement {
  // ESCAPE CHARACTER for LIKE is supported from Spark 3.0
  // The default escape character comes from the constructor of Like class.
  private val DEFAULT_LIKE_ESCAPE_CHAR: Char = '\\'

  /** Used mainly by QueryGeneration.convertExpression. This matches
    * a tuple of (Expression, Seq[Attribute]) representing the expression to
    * be matched and the fields that define the valid fields in the current expression
    * scope, respectively.
    *
    * @param expAttr A pair-tuple representing the expression to be matched and the
    *                attribute fields.
    * @return An option containing the translated SQL, if there is a match, or None if there
    *         is no match.
    */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[SnowflakeSQLStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case _: Ascii | _: Lower | _: Substring | _: StringLPad | _: StringRPad |
          _: StringTranslate | _: StringTrim | _: StringTrimLeft |
          _: StringTrimRight | _: Substring | _: Upper | _: Length =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case Concat(children) =>
        val rightSide =
          if (children.length > 2) Concat(children.drop(1)) else children(1)
        ConstantString("CONCAT") + blockStatement(
          convertStatement(children.head, fields) + "," +
            convertStatement(rightSide, fields)
        )

      // ESCAPE Char is supported from Spark 3.0
      case Like(left, right, escapeChar) =>
        val escapeClause =
          if (escapeChar == DEFAULT_LIKE_ESCAPE_CHAR) {
            ""
          } else {
            s"ESCAPE '${escapeChar}'"
          }
        convertStatement(left, fields) + "LIKE" + convertStatement(
          right,
          fields
        ) + escapeClause

//      case Sha2(left, right) =>
//        // For Spark sha2(), the expected input datatype type is Binary.
//        // For snowflake sha2(), the expected input datatype type is String.
//        // So it needs to some special conversion on the data type.
//        // Sha2() is supported to work on Binary and String type.
//        // 1. If the original input type is String, Spark will add
//        //    "CAST(C1 AS BINARY)" to the plan. The corresponding
//        //    Snowflake format is "CAST(C1 AS STRING)".
//        // 2. If the original input type is Binary, Spark will not add
//        //    CAST to the plan. The equivalent format for snowflake is
//        //    "HEX_DECODE_STRING( CAST( C1 AS STRING ))".
//        // Note: If Sha2() runs on const string/binary, spark has computed
//        //       the value. So Sha2() are not seen here.
//        val leftStmt = Option(left match {
//          case Cast(child, childType, _) =>
//            childType match {
//              case BinaryType =>
//                // The original input type is String
//                ConstantString("CAST") +
//                  blockStatement(convertStatement(child, fields) + "AS STRING")
//              case _ => null
//            }
//          case _ => {
//            // The original input type is Binary
//            ConstantString("HEX_DECODE_STRING") +
//              blockStatement(ConstantString("CAST") +
//                blockStatement(convertStatement(left, fields) + "AS STRING"))
//          }
//        })
//
//        if (leftStmt.isDefined) {
//          ConstantString(expr.prettyName.toUpperCase) +
//            blockStatement(leftStmt.get + "," +
//              convertStatement(right, fields))
//        } else {
//          null
//        }
//
//      case RegExpReplace(subject, regexp, rep) =>
//        def unescape(stmt: SnowflakeSQLStatement): String = {
//          stmt.toString.replaceAll("\\\\", "\\\\\\\\")
//        }
//
//        // The 'regexp' and 'rep' expression need to be converted
//        // before passing them to snowflake. For example,
//        // the original query is REGEXP_REPLACE("C2", "(\\d+)", "num")
//        // the 'regexp' here becomes "(\d+)". Before being passed to
//        // snowflake, it needs to be converted back to "(\\d+)".
//        // Note: REGEXP_REPLACE() with Const String has been optimized by Spark
//        //       so REGEXP_REPLACE() on Const String can't be seen here.
//        ConstantString(expr.prettyName.toUpperCase) +
//          blockStatement(
//            convertStatement(subject, fields) + "," +
//              unescape(convertStatement(regexp, fields)) + "," +
//              unescape(convertStatement(rep, fields)))

      case _ => null
    })
  }
}
