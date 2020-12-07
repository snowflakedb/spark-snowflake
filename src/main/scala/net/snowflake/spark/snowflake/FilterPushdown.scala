/*
 * Copyright 2015-2018 Snowflake Computing
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.spark.snowflake

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
  * Helper methods for pushing filters into Snowflake queries.
  */
private[snowflake] object FilterPushdown {

  /**
    * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
    * condition will be added to the WHERE clause. If none of the filters can be pushed down then
    * an empty string will be returned.
    *
    * @param schema the schema of the table being queried
    * @param filters an array of filters, the conjunction of which is the filter condition for the
    *                scan.
    */
  def buildWhereStatement(
    schema: StructType,
    filters: Seq[Filter],
    keepNameCase: Boolean = false
  ): SnowflakeSQLStatement = {
    val filterStatement =
      pushdowns.querygeneration
        .mkStatement(
          filters.flatMap(buildFilterStatement(schema, _, keepNameCase)),
          "AND"
        )
    if (filterStatement.isEmpty) EmptySnowflakeSQLStatement()
    else ConstantString("WHERE") + filterStatement
  }

  def buildFilterStatement(
    schema: StructType,
    filter: Filter,
    keepNameCase: Boolean
  ): Option[SnowflakeSQLStatement] = {

    // Builds an escaped value, based on the expected datatype
    def buildValueWithType(dataType: DataType,
                           value: Any): SnowflakeSQLStatement = {
      dataType match {
        case StringType =>
          StringVariable(
            Option(value).map(_.toString
              .replace("'", "''")
              .replace("\\", "\\\\")
            )) !
        case DateType =>
          StringVariable(Option(value).map(_.asInstanceOf[Date].toString)) + "::DATE"
        case TimestampType =>
          StringVariable(Option(value).map(_.asInstanceOf[Timestamp].toString)) + "::TIMESTAMP(3)"
        case _ =>
          value match {
            case v: Int => IntVariable(Some(v)) !
            case v: Long => LongVariable(Some(v)) !
            case v: Short => ShortVariable(Some(v)) !
            case v: Boolean => BooleanVariable(Some(v)) !
            case v: Float => FloatVariable(Some(v)) !
            case v: Double => DoubleVariable(Some(v)) !
            case v: Byte => ByteVariable(Some(v)) !
            case _ => ConstantStringVal(value) !
          }
      }
    }

    // Builds an escaped value, based on the value itself
    def buildValue(value: Any): SnowflakeSQLStatement = {
      value match {
        case x: String =>
          StringVariable(
            Option(x).map(_.replace("'", "''")
              .replace("\\", "\\\\"))
          ) !
        case x: Date => StringVariable(Option(x).map(_.toString)) + "::DATE"
        case x: Timestamp => StringVariable(Option(x).map(_.toString)) + "::TIMESTAMP(3)"
        case x: Int => IntVariable(Some(x)) !
        case x: Long => LongVariable(Some(x)) !
        case x: Short => ShortVariable(Some(x)) !
        case x: Boolean => BooleanVariable(Some(x)) !
        case x: Float => FloatVariable(Some(x)) !
        case x: Double => DoubleVariable(Some(x)) !
        case x: Byte => ByteVariable(Some(x)) !
        case _ => ConstantStringVal(value) !
      }
    }

    // Builds a simple comparison string
    def buildComparison(attr: String,
                        value: Any,
                        comparisonOp: String): Option[SnowflakeSQLStatement] = {
      val dataType = getTypeForAttribute(schema, attr)
      if (dataType.isEmpty) {
        return None
      }
      val sqlEscapedValue = buildValueWithType(dataType.get, value)
      Some(ConstantString(wrap(attr)) + comparisonOp + sqlEscapedValue)
    }

    def buildBinaryFilter(left: Filter,
                          right: Filter,
                          op: String): Option[SnowflakeSQLStatement] = {
      val leftStr = buildFilterStatement(schema, left, keepNameCase)
      val rightStr = buildFilterStatement(schema, right, keepNameCase)
      if (leftStr.isEmpty || rightStr.isEmpty) {
        None
      } else {
        Some(
          ConstantString("((") + leftStr.get + ")" + op + "(" + rightStr.get + "))"
        )
      }
    }

    def wrap(name: String): String =
      if (keepNameCase) Utils.quotedNameIgnoreCase(name)
      else name

    filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case In(attr, values: Array[Any]) =>
        val dataType = getTypeForAttribute(schema, attr).get
        val valueStrings =
          pushdowns.querygeneration
            .mkStatement(values.map(v => buildValueWithType(dataType, v)), ", ")
        Some(
          ConstantString("(") + wrap(attr) + "IN" + "(" + valueStrings + "))"
        )
      case IsNull(attr) => Some(ConstantString("(") + wrap(attr) + "IS NULL)")
      case IsNotNull(attr) =>
        Some(ConstantString("(") + wrap(attr) + "IS NOT NULL)")
      case And(left, right) =>
        buildBinaryFilter(left, right, "AND")
      case Or(left, right) =>
        buildBinaryFilter(left, right, "OR")
      case Not(child) =>
        val childStr = buildFilterStatement(schema, child, keepNameCase)
        if (childStr.isEmpty) None
        else Some(ConstantString("(NOT (") + childStr.get + "))")
      case StringStartsWith(attr, value) =>
        Some(
          ConstantString("STARTSWITH(") + wrap(attr) + "," + buildValue(value) + ")"
        )
      case StringEndsWith(attr, value) =>
        Some(
          ConstantString("ENDSWITH(") + wrap(attr) + "," + buildValue(value) + ")"
        )
      case StringContains(attr, value) =>
        Some(
          ConstantString("CONTAINS(") + wrap(attr) + "," + buildValue(value) + ")"
        )
      case _ => None
    }
  }

  /**
    * Use the given schema to look up the attribute's data type. Returns None if the attribute could
    * not be resolved.
    */
  private def getTypeForAttribute(schema: StructType,
                                  attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
}
