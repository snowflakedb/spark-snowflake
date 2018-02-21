package net.snowflake.spark.snowflake.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, CaseWhenCodegen, Cast, Descending, Expression, Floor, If, In, InSet, Literal, MakeDecimal, RangeFrame, RowFrame, ScalarSubquery, ShiftLeft, ShiftRight, SortOrder, SpecifiedWindowFrame, UnscaledValue, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.PartitionSpecContext
import org.apache.spark.sql.types.{Decimal, _}
import org.datanucleus.store.rdbms.sql.expression.NullLiteral

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
      case CaseWhenCodegen(branches, elseValue) =>
        val cases = "CASE " + branches
            .map(
              b =>
                "WHEN " + convertExpression(b._1, fields) + " THEN " + convertExpression(
                  b._2,
                  fields))
            .mkString(" ")
        if (elseValue.isDefined)
          block(
            cases + " ELSE " + convertExpression(elseValue.get, fields) + " END")
        else block(cases + " END")
      case Cast(child, t) =>
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

      case SortOrder(child, Ascending, _) =>
        block(convertExpression(child, fields)) + " ASC"
      case SortOrder(child, Descending, _) =>
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
