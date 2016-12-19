package net.snowflake.spark.snowflake.pushdowns.QueryGeneration

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Cast,
  Expression
}
import org.apache.spark.sql.types._

/**
  * Extractors for everything else.
  */
private[QueryGeneration] object MiscExpression {

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

    expr match {
      case Alias(child: Expression, name: String) =>
        Some(block(convertExpression(child, fields), name))
      case Cast(child, t) =>
        getCastType(t) match {
          case None =>
            Some(convertExpression(child, fields))
          case Some(cast) =>
            Some(
              "CAST" + block(convertExpression(child, fields) + "AS " + cast))
        }

      case _ => None
    }
  }

  /**
    * Attempts a best effort conversion from a SparkType
    * to a Snowflake type to be used in a Cast.
    *
    */
  private final def getCastType(t: DataType): Option[String] = t match {
    case StringType    => Some("VARCHAR")
    case BinaryType    => Some("BINARY")
    case DateType      => Some("DATE")
    case TimestampType => Some("TIMESTAMP")
    case d: DecimalType =>
      Some("DECIMAL(" + d.precision + ", " + d.scale + ")")
    case IntegerType | LongType => Some("NUMBER")
    case FloatType              => Some("FLOAT")
    case DoubleType             => Some("DOUBLE")
    case _                      => None
  }
}
