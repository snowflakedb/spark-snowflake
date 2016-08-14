/*
 * Copyright 2015-2016 Snowflake Computing
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

package com.snowflakedb.spark.snowflakedb

import java.sql.{Date, Timestamp}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Helper methods for pushing filters into Snowflake queries.
 */
private[snowflakedb] object FilterPushdown {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   */
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  /**
   * Attempt to convert the given filter into a SQL expression. Returns None if the expression
   * could not be converted.
   */
  private def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {

    // Builds an escaped value, based on the expected datatype
    def buildValueWithType(dataType: DataType, internalValue: Any): String = {
      val value: Any = {
        // Workaround for SPARK-10195: prior to Spark 1.5.0, the Data Sources API exposed internal
        // types, so we must perform conversions if running on older versions:
        if (SPARK_VERSION < "1.5.0") {
          CatalystTypeConverters.convertToScala(internalValue, dataType)
        } else {
          internalValue
        }
      }
      dataType match {
        case StringType => s"'${value.toString.replace("'", "''").replace("\\", "\\\\")}'"
        case DateType => s"'${value.asInstanceOf[Date]}'::DATE"
        case TimestampType => s"'${value.asInstanceOf[Timestamp]}'::TIMESTAMP(3)"
        case _ => value.toString
      }
    }

    // Builds an escaped value, based on the value itself
    def buildValue(value: Any): String = {
      value match {
        case _: String => s"'${value.toString.replace("'", "''").replace("\\", "\\\\")}'"
        case _: Date => s"'${value.asInstanceOf[Date]}'::DATE"
        case _: Timestamp=> s"'${value.asInstanceOf[Timestamp]}'::TIMESTAMP(3)"
        case _ => value.toString
      }
    }

    // Builds a simple comparison string
    def buildComparison(attr: String, internalValue: Any, comparisonOp: String): Option[String] = {
      val dataType = getTypeForAttribute(schema, attr)
      if (dataType.isEmpty)
        return None
      val sqlEscapedValue = buildValueWithType(dataType.get, internalValue)
      Some(s""""$attr" $comparisonOp $sqlEscapedValue""")
    }

    def buildBinaryFilter(left: Filter, right: Filter, op: String) : Option[String] = {
      val leftStr = buildFilterExpression(schema, left)
      val rightStr = buildFilterExpression(schema, right)
      if (leftStr.isEmpty || rightStr.isEmpty)
        None
      else
        Some(s"""((${leftStr.get}) $op (${rightStr.get}))""")
    }

    filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case In(attr, values: Array[Any]) =>
        val dataType = getTypeForAttribute(schema, attr).get
        val valueStrings = values
            .map(v => buildValueWithType(dataType, v))
            .mkString(", ")
        Some(s"""("$attr" IN ($valueStrings))""")
      case IsNull(attr) => Some(s"""("$attr" IS NULL)""")
      case IsNotNull(attr) => Some(s"""("$attr" IS NOT NULL)""")
      case And(left, right) =>
        buildBinaryFilter(left, right, "AND")
      case Or(left, right) =>
        buildBinaryFilter(left, right, "OR")
      case Not(child) =>
        val childStr = buildFilterExpression(schema, child)
        if (childStr.isEmpty)
          None
        else
          Some(s"""(NOT (${childStr.get}))""")
      case StringStartsWith(attr, value) =>
        Some(s"""STARTSWITH("$attr", ${buildValue(value)})""")
      case StringEndsWith(attr, value) =>
        Some(s"""ENDSWITH("$attr", ${buildValue(value)})""")
      case StringContains(attr, value) =>
        Some(s"""CONTAINS("$attr", ${buildValue(value)})""")
      case _ => None
    }
  }

  /**
   * Use the given schema to look up the attribute's data type. Returns None if the attribute could
   * not be resolved.
   */
  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
}
