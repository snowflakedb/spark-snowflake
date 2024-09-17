package net.snowflake.spark.snowflake.io

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder.{BaseFieldTypeBuilder, BaseTypeBuilder, FieldDefault, RecordBuilder, nullable}
import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import org.apache.spark.sql.types._

import java.io.OutputStream

object ParquetUtils {
  private val nameSpace = "snowflake"

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
        builder.intBuilder()
          .prop("logicalType", "date")
          .endInt()
      case TimestampType | TimestampNTZType =>
        builder.longBuilder()
          .prop("logicalType", " timestamp-nanos")
          .endLong()
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
        builder.intBuilder()
          .prop("logicalType", "date")
          .endInt()
      case TimestampType | TimestampNTZType =>
        builder.longBuilder()
          .prop("logicalType", "timestamp-nanos")
          .endLong()
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