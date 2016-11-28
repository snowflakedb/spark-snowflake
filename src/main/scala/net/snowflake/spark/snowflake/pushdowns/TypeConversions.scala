package net.snowflake.spark.snowflake.pushdowns


import java.sql.{ResultSet, ResultSetMetaData, Types => JDBCTypes}

import com.google.common.primitives.UnsignedLongs
import org.apache.spark.sql.types._

object TypeConversions {
  val MEMSQL_DECIMAL_MAX_PRECISION = 65
  val MEMSQL_DECIMAL_MAX_SCALE = 30

  def decimalTypeToMySQLType(decimal: DecimalType): String = {
    val precision = Math.min(MEMSQL_DECIMAL_MAX_PRECISION, decimal.precision)
    val scale = Math.min(MEMSQL_DECIMAL_MAX_SCALE, decimal.scale)
    s"DECIMAL($precision, $scale)"
  }

  /**
    * Find the appropriate MemSQL type from a SparkSQL type.
    *
    * Most types share the same name but there are a few special cases because
    * the types don't align perfectly.
    *
    * @param dataType A SparkSQL Type
    * @return Corresponding MemSQL type
    */
  def DataFrameTypeToMemSQLTypeString(dataType: DataType): String = {
    dataType match {
      case ShortType => "SMALLINT"
      case LongType => "BIGINT"
      case BooleanType => "BOOLEAN"
      case StringType => "TEXT"
      case BinaryType => "BLOB"
      case dt: DecimalType => decimalTypeToMySQLType(dt)
      case _ => dataType.typeName
    }
  }

  /**
    * Attempts a best effort conversion from a SparkType
    * to a MemSQLType to be used in a Cast.
    *
    * @note Will raise a match error for unsupported casts
    */
  def DataFrameTypeToMemSQLCastType(t: DataType): Option[String] = t match {
    case StringType => Some("CHAR")
    case BinaryType => Some("BINARY")
    case DateType => Some("DATE")
    case TimestampType => Some("DATE")
    case decimal: DecimalType => Some(decimalTypeToMySQLType(decimal))
    case LongType => Some("SIGNED")
    case FloatType => Some("DECIMAL(14, 7)")
    case DoubleType => Some("DECIMAL(30, 15)")
    case _ => None
  }

  def JDBCTypeToDataFrameType(rsmd: ResultSetMetaData, ix: Int): DataType = {
    rsmd.getColumnType(ix) match {
      case JDBCTypes.TINYINT => ShortType
      case JDBCTypes.SMALLINT => ShortType
      case JDBCTypes.INTEGER => IntegerType
      case JDBCTypes.BIGINT => rsmd.isSigned(ix) match {
        case true => LongType
        case _ => IntegerType
      }

      case JDBCTypes.DOUBLE => DoubleType
      case JDBCTypes.NUMERIC => DoubleType
      case JDBCTypes.REAL => FloatType
      case JDBCTypes.DECIMAL => DecimalType(
        math.min(DecimalType.MAX_PRECISION, rsmd.getPrecision(ix)),
        math.min(DecimalType.MAX_SCALE, rsmd.getScale(ix))
      )

      case JDBCTypes.TIMESTAMP => TimestampType
      case JDBCTypes.DATE => DateType
      // MySQL TIME type is represented as a long in milliseconds
      case JDBCTypes.TIME => LongType

      case JDBCTypes.CHAR => StringType
      case JDBCTypes.VARCHAR => StringType
      case JDBCTypes.NVARCHAR => StringType
      case JDBCTypes.LONGVARCHAR => StringType
      case JDBCTypes.LONGNVARCHAR => StringType

      case JDBCTypes.BIT => BinaryType
      case JDBCTypes.BINARY => BinaryType
      case JDBCTypes.VARBINARY => BinaryType
      case JDBCTypes.LONGVARBINARY => BinaryType
      case JDBCTypes.BLOB => BinaryType

      case _ => throw new IllegalArgumentException("Can't translate type " + rsmd.getColumnTypeName(ix))
    }
  }

  def GetJDBCValue(dataType: Int, isSigned: Boolean, ix: Int, row: ResultSet): Any = {
    val result = dataType match {
      case JDBCTypes.TINYINT => row.getShort(ix)
      case JDBCTypes.SMALLINT => row.getShort(ix)
      case JDBCTypes.INTEGER => row.getInt(ix)
      case JDBCTypes.BIGINT => isSigned match {
        case true => row.getLong(ix)
        case _ => Option(row.getString(ix)).map(UnsignedLongs.parseUnsignedLong).getOrElse(null)
      }

      case JDBCTypes.DOUBLE => row.getDouble(ix)
      case JDBCTypes.NUMERIC => row.getDouble(ix)
      case JDBCTypes.REAL => row.getFloat(ix)
      case JDBCTypes.DECIMAL => row.getBigDecimal(ix)

      case JDBCTypes.TIMESTAMP => row.getTimestamp(ix)
      case JDBCTypes.DATE => row.getDate(ix)
      // MySQL TIME type is represented as a long in milliseconds
      case JDBCTypes.TIME => row.getTime(ix).getTime

      case JDBCTypes.CHAR => row.getString(ix)
      case JDBCTypes.VARCHAR => row.getString(ix)
      case JDBCTypes.NVARCHAR => row.getString(ix)
      case JDBCTypes.LONGNVARCHAR => row.getString(ix)
      case JDBCTypes.LONGVARCHAR => row.getString(ix)

      case JDBCTypes.BIT => row.getBytes(ix)
      case JDBCTypes.BINARY => row.getBytes(ix)
      case JDBCTypes.VARBINARY => row.getBytes(ix)
      case JDBCTypes.LONGVARBINARY => row.getBytes(ix)
      case JDBCTypes.BLOB => row.getBytes(ix)

      case _ => throw new IllegalArgumentException("Can't translate type " + dataType.toString)
    }

    if (row.wasNull) null else result
  }
}