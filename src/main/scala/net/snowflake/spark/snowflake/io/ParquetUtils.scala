package net.snowflake.spark.snowflake.io

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder.{BaseFieldTypeBuilder, BaseTypeBuilder, FieldDefault, RecordBuilder}
import org.apache.avro.generic.GenericData
import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.io.OutputStream
import java.nio.ByteBuffer
import java.time.ZoneOffset
import scala.collection.mutable
import scala.collection.JavaConverters._

object ParquetUtils {
  private val nameSpace = "snowflake"

  def rowToAvroRecord(row: Row,
                      schema: Schema,
                      snowflakeStyleSchema: StructType,
                      params: MergedParameters): GenericData.Record = {
    val record = new GenericData.Record(schema)
    row.toSeq.zip(snowflakeStyleSchema.names).foreach {
      case (row: Row, name) =>
        record.put(name,
          rowToAvroRecord(
            row,
            schema.getField(name).schema().getTypes.get(0),
            snowflakeStyleSchema(name).dataType.asInstanceOf[StructType],
            params
          ))
      case (map: scala.collection.immutable.Map[Any, Any], name) =>
        record.put(name, map.asJava)
      case (str: String, name) =>
        record.put(name, if (params.trimSpace) str.trim else str)
      case (arr: mutable.WrappedArray[Any], name) =>
        record.put(name, arr.toArray)
      case (decimal: java.math.BigDecimal, name) =>
        record.put(name, ByteBuffer.wrap(decimal.unscaledValue().toByteArray))
      case (timestamp: java.sql.Timestamp, name) =>
        record.put(name, timestamp.toString)
      case (date: java.sql.Date, name) =>
        record.put(name, date.toString)
      case (date: java.time.LocalDateTime, name) =>
        record.put(name, date.toEpochSecond(ZoneOffset.UTC))
      case (value, name) => record.put(name, value)
    }
    record
  }

  def convertStructToAvro(structType: StructType): Schema =
    convertStructToAvro(
      structType,
      SchemaBuilder.record("root").namespace(nameSpace)
    )
  private def convertStructToAvro[T](structType: StructType,
                                     builder: RecordBuilder[T]): T = {
    val filedAssembler = builder.fields()
    structType.fields.foreach (field => {
      val newField = filedAssembler.name(field.name).`type`()
      convertFieldToAvro(
        field.dataType,
        if (field.nullable) newField.nullable() else newField,
        field.name
      ).noDefault()
    })
    filedAssembler.endRecord()
  }

  private def convertFieldToAvro[T](
                                     dataType: DataType,
                                     builder: BaseFieldTypeBuilder[T],
                                     name: String
                                   ): FieldDefault[T, _] = {
    dataType match {
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      // todo: should have a special handler
      case dc: DecimalType =>
        builder.bytesBuilder()
          .prop("logicalType", "decimal")
          .prop("precision", dc.precision)
          .prop("scale", dc.scale)
          .endBytes()
      case StringType => builder.stringType()
      case BinaryType => builder.bytesType()
      case BooleanType => builder.booleanType()
      case DateType =>
        builder.stringBuilder()
          .prop("logicalType", "date")
          .endString()
      case TimestampType | TimestampNTZType =>
        builder.stringBuilder()
          .prop("logicalType", " timestamp-micros")
          .endString()
      case ArrayType(elementType, nullable) =>
        builder.array().items(
          convertTypeToAvro(
            elementType,
            getSchemaBuilder(nullable),
            name
          )
        )
      case MapType(StringType, valueType, valueContainsNull) =>
        builder.map().values(
          convertTypeToAvro(
            valueType,
            getSchemaBuilder(valueContainsNull),
            name
          )
        )
      case struct: StructType =>
        convertStructToAvro(
          struct,
          builder.record(name).namespace(nameSpace)
        )
      case _ =>
        throw new UnsupportedOperationException(s"Unexpected type: $dataType")
    }
  }

  private def convertTypeToAvro[T](
                                    dataType: DataType,
                                    builder: BaseTypeBuilder[T],
                                    name: String
                                  ): T = {
    dataType match {
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      // todo: should have a special handler
      case dc: DecimalType =>
        builder.bytesBuilder()
          .prop("logicalType", "decimal")
          .prop("precision", dc.precision)
          .prop("scale", dc.scale)
          .endBytes()
      case StringType => builder.stringType()
      case BinaryType => builder.bytesType()
      case BooleanType => builder.booleanType()
      case DateType =>
        builder.stringBuilder()
          .prop("logicalType", "date")
          .endString()
      case TimestampType | TimestampNTZType =>
        builder.stringBuilder()
          .prop("logicalType", "timestamp-micros")
          .endString()
      case ArrayType(elementType, nullable) =>
        builder.array().items(
          convertTypeToAvro(
            elementType,
            getSchemaBuilder(nullable),
            name
          )
        )
      case MapType(StringType, valueType, valueContainsNull) =>
        builder.map().values(
          convertTypeToAvro(
            valueType,
            getSchemaBuilder(valueContainsNull),
            name
          )
        )
      case struct: StructType =>
        convertStructToAvro(
          struct,
          builder.record(name).namespace(nameSpace)
        )
      case _ =>
        throw new UnsupportedOperationException(s"Unexpected type: $dataType")
    }
  }

  private def getSchemaBuilder(isNullable: Boolean): BaseTypeBuilder[Schema] =
    if (isNullable) SchemaBuilder.nullable() else SchemaBuilder.builder()

  class StreamOutputFile(output: OutputStream) extends OutputFile {

    override def create(blockSizeHint: Long): PositionOutputStream =
      createOrOverwrite(blockSizeHint)

    override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = {
      new PositionOutputStream {
        private var pos: Long = 0
        override def getPos: Long = pos

        override def write(b: Int): Unit = {
          output.write(b)
          pos += 1
        }

        override def flush(): Unit = output.flush()

        override def close(): Unit = output.close()
      }
    }

    override def supportsBlockSize(): Boolean = true

    override def defaultBlockSize(): Long = 4000000
  }
}