package net.snowflake.spark.snowflake.pushdowns.querygeneration

import net.snowflake.spark.snowflake.{ConstantString, EmptySnowflakeSQLStatement, IntVariable, SnowflakeSQLStatement}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, Cast, Descending, Expression, If, In, InSet, Literal, MakeDecimal, ScalarSubquery, ShiftLeft, ShiftRight, SortOrder, UnscaledValue, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.types.{Decimal, _}

/** Extractors for everything else. */
private[querygeneration] object MiscExpression {

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
  def unapply(expAttr: (Expression, Seq[Attribute])): Option[String] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case Alias(child: Expression, name: String) =>
        block(convertExpression(child, fields), name)
      case Cast(child, t, _) =>
        getCastType(t) match {
          case None =>
            convertExpression(child, fields)
          case Some(cast) =>
            "CAST" + block(convertExpression(child, fields) + " AS " + cast)
        }
      case If(child, trueValue, falseValue) =>
        "IFF" + block(convertExpressions(fields, child, trueValue, falseValue))

      case In(child, list) => {
        block(
          convertExpression(child, fields) + " IN " + block(
            convertExpressions(fields, list: _*)))
      }

      case InSet(child, hset) => {
        convertExpression(In(child, setToExpr(hset)), fields)
      }

      case MakeDecimal(child, precision, scale) =>
        "TO_DECIMAL " + block(block(
          convertExpression(child, fields) + "/ POW(10, " + scale + ")") + s", $precision, $scale")

      case ShiftLeft(col, num) =>
        "BITSHIFTLEFT" + block(convertExpressions(fields, col, num))

      case ShiftRight(col, num) =>
        "BITSHIFTRIGHT" + block(convertExpressions(fields, col, num))

      case SortOrder(child, Ascending, _, _) =>
        block(convertExpression(child, fields)) + " ASC"
      case SortOrder(child, Descending, _, _) =>
        block(convertExpression(child, fields)) + " DESC"

      case ScalarSubquery(subquery, _, _) =>
        block(new QueryBuilder(subquery).query)

      case UnscaledValue(child) => {
        child.dataType match {
          case d: DecimalType => {
            block(
              convertExpression(child, fields) + " * POW(10," + d.scale + ")")
          }
          case _ => null
        }
      }

      case WindowExpression(func, spec) =>
        convertExpression(func, fields) + " OVER " + windowBlock(spec, fields)

      case _ => null
    })
  }

  private final def windowBlock(spec: WindowSpecDefinition,
                                fields: Seq[Attribute]) = {
    val partitionBy =
      if (spec.partitionSpec.isEmpty) ""
      else
        "PARTITION BY " + spec.partitionSpec
          .map(expr => convertExpression(expr, fields))
          .mkString(", ")

    val orderBy =
      if (spec.orderSpec.isEmpty) ""
      else
        " ORDER BY " + spec.orderSpec
          .map(expr => convertExpression(expr, fields))
          .mkString(", ")

    val fromTo =
      if (spec.orderSpec.isEmpty) ""
      else " " + spec.frameSpecification.toString

    /*
    val fromTo = spec.frameSpecification match {
      case SpecifiedWindowFrame(frameType, frameStart, frameEnd) => {
        frameType match {
          case RowFrame => " ROWS "
          case RangeFrame => " RANGE "
        }
      }
    }
     */

    block(partitionBy + orderBy + fromTo)
  }

  private final def setToExpr(set: Set[Any]): Seq[Expression] = {
    set.map { item =>
      item match {
        case d: Decimal    => Literal(d, DecimalType(d.precision, d.scale))
        case s: String     => Literal(s, StringType)
        case e: Expression => e
      }
    }.toSeq
  }

  /** Attempts a best effort conversion from a SparkType
    * to a Snowflake type to be used in a Cast.
    */
  private final def getCastType(t: DataType): Option[String] =
    Option(t match {
      case StringType    => "VARCHAR"
      case BinaryType    => "BINARY"
      case DateType      => "DATE"
      case TimestampType => "TIMESTAMP"
      case d: DecimalType =>
        "DECIMAL(" + d.precision + ", " + d.scale + ")"
      case IntegerType | LongType => "NUMBER"
      case FloatType              => "FLOAT"
      case DoubleType             => "DOUBLE"
      case _                      => null
    })
}

private[querygeneration] object MiscStatement {

  def unapply(expAttr: (Expression, Seq[Attribute])): Option[SnowflakeSQLStatement] = {
    val expr   = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case Alias(child: Expression, name: String) =>
        blockStatement(convertStatement(child, fields), name)
      case Cast(child, t, _) =>
        getCastType(t) match {
          case None =>
            convertStatement(child, fields)
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
        }
      case If(child, trueValue, falseValue) =>
        ConstantString("IFF") +
          blockStatement(convertStatements(fields, child, trueValue, falseValue))

      case In(child, list) => {
        blockStatement(
          convertStatement(child, fields) + "IN" +
            blockStatement(convertStatements(fields, list: _*)))
      }

      case InSet(child, hset) => {
        convertStatement(In(child, setToExpr(hset)), fields)
      }

      case MakeDecimal(child, precision, scale) =>
        ConstantString("TO_DECIMAL") + blockStatement(
          blockStatement(convertStatement(child, fields) + "/ POW(10," +
            IntVariable(scale) + ")") + "," + IntVariable(precision) + "," +
            IntVariable(scale)
        )
      case ShiftLeft(col, num) =>
        ConstantString("BITSHIFTLEFT") +
          blockStatement(convertStatements(fields, col, num))

      case ShiftRight(col, num) =>
        ConstantString("BITSHIFTRIGHT") +
          blockStatement(convertStatements(fields, col, num))

      case SortOrder(child, Ascending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "ASC"
      case SortOrder(child, Descending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "DESC"

      case ScalarSubquery(subquery, _, _) =>
        blockStatement(new QueryBuilder(subquery).statement)

      case UnscaledValue(child) => {
        child.dataType match {
          case d: DecimalType => {
            blockStatement(
              convertStatement(child, fields) + "* POW(10," + IntVariable(d.scale) + ")")
          }
          case _ => null
        }
      }

      case WindowExpression(func, spec) =>
        convertStatement(func, fields) + "OVER" + windowBlock(spec, fields)

      case _ => null
    })
  }

  private final def windowBlock(spec: WindowSpecDefinition,
                                fields: Seq[Attribute]): SnowflakeSQLStatement = {
    val partitionBy =
      if (spec.partitionSpec.isEmpty) EmptySnowflakeSQLStatement()
      else
        ConstantString("PARTITION BY") +
          mkStatement(spec.partitionSpec.map(convertStatement(_, fields)), ",")


    val orderBy =
      if (spec.orderSpec.isEmpty) EmptySnowflakeSQLStatement()
      else
        ConstantString("ORDER BY") +
          mkStatement(spec.orderSpec.map(convertStatement(_, fields)), ",")

    val fromTo =
      if (spec.orderSpec.isEmpty) EmptySnowflakeSQLStatement()
      else  ConstantString(spec.frameSpecification.toString) !

    blockStatement(partitionBy + orderBy + fromTo)
  }

  private final def setToExpr(set: Set[Any]): Seq[Expression] = {
    set.map { item =>
      item match {
        case d: Decimal    => Literal(d, DecimalType(d.precision, d.scale))
        case s: String     => Literal(s, StringType)
        case e: Expression => e
      }
    }.toSeq
  }

  /** Attempts a best effort conversion from a SparkType
    * to a Snowflake type to be used in a Cast.
    */
  private final def getCastType(t: DataType): Option[String] =
    Option(t match {
      case StringType    => "VARCHAR"
      case BinaryType    => "BINARY"
      case DateType      => "DATE"
      case TimestampType => "TIMESTAMP"
      case d: DecimalType =>
        "DECIMAL(" + d.precision + ", " + d.scale + ")"
      case IntegerType | LongType => "NUMBER"
      case FloatType              => "FLOAT"
      case DoubleType             => "DOUBLE"
      case _                      => null
    })
}
