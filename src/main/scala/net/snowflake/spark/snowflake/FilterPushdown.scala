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
import org.slf4j.LoggerFactory

/**
 * Helper methods for pushing filters into Snowflake queries.
 */
private[snowflake] object FilterPushdown {

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


  def buildWhereStatement(schema: StructType, filters: Seq[Filter]): SnowflakeSQLStatement = {
    val filterStatement =
      pushdowns.querygeneration
        .mkStatement(filters.flatMap(buildFilterStatement(schema, _)), "AND")
    if(filterStatement.isEmpty) EmptySnowflakeSQLStatement() else ConstantString("WHERE") + filterStatement
  }

  def buildFilterStatement(schema: StructType, filter: Filter): Option[SnowflakeSQLStatement] = {

    // Builds an escaped value, based on the expected datatype
    def buildValueWithType(dataType: DataType, value: Any): SnowflakeSQLStatement = {
      dataType match {
        case StringType => StringVariable(value.toString
          .replace("'", "''")
          .replace("\\", "\\\\")) !
        case DateType => StringVariable(value.asInstanceOf[Date].toString) + "::DATE"
        case TimestampType => StringVariable(value.asInstanceOf[Timestamp].toString) + "::TIMESTAMP(3)"
        case IntegerType => IntVariable(value.asInstanceOf[Int]) !
        case LongType => LongVariable(value.asInstanceOf[Long]) !
        case ShortType => ShortVariable(value.asInstanceOf[Short]) !
        case BooleanType => BooleanVariable(value.asInstanceOf[Boolean]) !
        case FloatType => FloatVariable(value.asInstanceOf[Float]) !
        case DoubleType => DoubleVariable(value.asInstanceOf[Double]) !
        case ByteType => ByteVariable(value.asInstanceOf[Byte]) !
        case _  => ConstantString(value.toString) !
      }
    }

    // Builds an escaped value, based on the value itself
    def buildValue(value: Any): SnowflakeSQLStatement = {
      value match {
        case x: String => StringVariable(x
          .replace("'", "''")
          .replace("\\", "\\\\")) !
        case x: Date => StringVariable(x.toString) + "::DATE"
        case x: Timestamp => StringVariable(x.toString) + "::TIMESTAMP(3)"
        case x: Int => IntVariable(x) !
        case x: Long => LongVariable(x) !
        case x: Short => ShortVariable(x) !
        case x: Boolean => BooleanVariable(x) !
        case x: Float => FloatVariable(x) !
        case x: Double => DoubleVariable(x) !
        case x: Byte => ByteVariable(x) !
        case _  => ConstantString(value.toString) !
      }
    }

    // Builds a simple comparison string
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[SnowflakeSQLStatement] = {
      val dataType = getTypeForAttribute(schema, attr)
      if (dataType.isEmpty) {
        return None
      }
      val sqlEscapedValue = buildValueWithType(dataType.get, value)
      Some(ConstantString(attr) + comparisonOp + sqlEscapedValue)
    }

    //todo
    def buildBinaryFilter(left: Filter, right: Filter, op: String) : Option[SnowflakeSQLStatement] = {
      val leftStr = buildFilterStatement(schema, left)
      val rightStr = buildFilterStatement(schema, right)
      if (leftStr.isEmpty || rightStr.isEmpty) {
        None
      } else {
        Some(ConstantString("((") + leftStr.get + ")" + op + "(" + rightStr.get + "))")
      }
    }

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
        Some(ConstantString("(") + attr + "IN" + "(" + valueStrings + "))")
      case IsNull(attr) => Some(ConstantString("(") + attr + "IS NULL)")
      case IsNotNull(attr) => Some(ConstantString("(") + attr + "IS NOT NULL)")
      case And(left, right) =>
        buildBinaryFilter(left, right, "AND")
      case Or(left, right) =>
        buildBinaryFilter(left, right, "OR")
      case Not(child) =>
        val childStr = buildFilterExpression(schema, child)
        if (childStr.isEmpty) None else Some(ConstantString("(NOT (") + childStr.get + "))")
      case StringStartsWith(attr, value) =>
        Some(ConstantString("STARTSWITH(") + attr + "," + buildValue(value) + ")")
      case StringEndsWith(attr, value) =>
        Some(ConstantString("ENDSWITH(") + attr + "," + buildValue(value) + ")")
      case StringContains(attr, value) =>
        Some(ConstantString("CONTAINS(") + attr + "," + buildValue(value) + ")")
      case _ => None
    }
  }


  /**
   * Attempt to convert the given filter into a SQL expression. Returns None if the expression
   * could not be converted.
   */
  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {

    // Builds an escaped value, based on the expected datatype
    def buildValueWithType(dataType: DataType, value: Any): String = {
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
        case _: Timestamp => s"'${value.asInstanceOf[Timestamp]}'::TIMESTAMP(3)"
        case _ => value.toString
      }
    }

    // Builds a simple comparison string
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      val dataType = getTypeForAttribute(schema, attr)
      if (dataType.isEmpty) {
        return None
      }
      val sqlEscapedValue = buildValueWithType(dataType.get, value)
      Some(s"""$attr $comparisonOp $sqlEscapedValue""")
    }

    def buildBinaryFilter(left: Filter, right: Filter, op: String) : Option[String] = {
      val leftStr = buildFilterExpression(schema, left)
      val rightStr = buildFilterExpression(schema, right)
      if (leftStr.isEmpty || rightStr.isEmpty) {
        None
      } else {
        Some(s"""((${leftStr.get}) $op (${rightStr.get}))""")
      }
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
        Some(s"""($attr IN ($valueStrings))""")
      case IsNull(attr) => Some(s"""($attr IS NULL)""")
      case IsNotNull(attr) => Some(s"""($attr IS NOT NULL)""")
      case And(left, right) =>
        buildBinaryFilter(left, right, "AND")
      case Or(left, right) =>
        buildBinaryFilter(left, right, "OR")
      case Not(child) =>
        val childStr = buildFilterExpression(schema, child)
        if (childStr.isEmpty) {
          None
        } else {
          Some(s"""(NOT (${childStr.get}))""")
        }
      case StringStartsWith(attr, value) =>
        Some(s"""STARTSWITH($attr, ${buildValue(value)})""")
      case StringEndsWith(attr, value) =>
        Some(s"""ENDSWITH($attr, ${buildValue(value)})""")
      case StringContains(attr, value) =>
        Some(s"""CONTAINS($attr, ${buildValue(value)})""")
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
