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
    row.toSeq.zip(snowflakeStyleSchema.names).foreach { case (value, name) =>
      val fieldSchema = nonNullSchema(schema.getField(name).schema())
      val sparkFieldType = snowflakeStyleSchema(name).dataType
      record.put(name, convertValue(value, fieldSchema, sparkFieldType, params))
    }
    record
  }

  private def nonNullSchema(s: Schema): Schema =
    if (s.getType == Schema.Type.UNION) s.getTypes.asScala.find(_.getType != Schema.Type.NULL).getOrElse(s)
    else s

  private def convertValue(value: Any,
                           avroSchema: Schema,
                           sparkType: DataType,
                           params: MergedParameters): Any = {
    if (value == null) return null

    sparkType match {
      case structType: StructType =>
        val recordSchema = nonNullSchema(avroSchema)
        rowToAvroRecord(value.asInstanceOf[Row], recordSchema, structType, params)

      case ArrayType(elementType, _) =>
        val arraySchema = nonNullSchema(avroSchema)
        val elementSchema = nonNullSchema(arraySchema.getElementType)
        val iterable: Iterable[Any] = value match {
          case wa: mutable.WrappedArray[_] => wa.asInstanceOf[mutable.WrappedArray[Any]].toSeq
          case a: Array[_] => a.asInstanceOf[Array[Any]].toSeq
          case s: Seq[_] => s.asInstanceOf[Seq[Any]]
          case jl: java.util.List[_] => jl.asScala.asInstanceOf[mutable.Buffer[Any]].toSeq
          case other => Seq(other)
        }
        // Return a JVM array to match Avro ListWriter.writeJavaArray path
        iterable
          .map(elem => convertValue(elem, elementSchema, elementType, params).asInstanceOf[AnyRef])
          .toArray[AnyRef]

      case MapType(StringType, valueType, _) =>
        val mapSchema = nonNullSchema(avroSchema)
        val valueSchema = nonNullSchema(mapSchema.getValueType)
        val scalaMap: Map[String, Any] = value match {
          case m: scala.collection.immutable.Map[_, _] =>
            m.asInstanceOf[scala.collection.immutable.Map[Any, Any]]
              .map { case (k, v) => k.toString -> v }
          case m: scala.collection.mutable.Map[_, _] =>
            m.asInstanceOf[scala.collection.mutable.Map[Any, Any]].toMap
              .map { case (k, v) => k.toString -> v }
          case jm: java.util.Map[_, _] =>
            jm.asScala.toMap.asInstanceOf[Map[Any, Any]].map { case (k, v) => k.toString -> v }
          case other =>
            throw new IllegalArgumentException(s"Unexpected map value: ${other.getClass}")
        }
        scalaMap.map { case (k, v) =>
          k -> convertValue(v, valueSchema, valueType, params)
        }.asJava

      case dc: DecimalType =>
        value match {
          case jbd: java.math.BigDecimal => ByteBuffer.wrap(jbd.unscaledValue().toByteArray)
          case sbd: scala.math.BigDecimal => ByteBuffer.wrap(sbd.bigDecimal.unscaledValue().toByteArray)
          case d: org.apache.spark.sql.types.Decimal =>
            ByteBuffer.wrap(d.toJavaBigDecimal.unscaledValue().toByteArray)
          case other =>
            // Fallback: attempt to parse string/number into BigDecimal
            val bd = new java.math.BigDecimal(other.toString)
            ByteBuffer.wrap(bd.unscaledValue().toByteArray)
        }

      case StringType =>
        val s = value.toString
        if (params.trimSpace) s.trim else s

      case TimestampType =>
        value.asInstanceOf[java.sql.Timestamp].toString

      case DateType =>
        value.asInstanceOf[java.sql.Date].toString

      case _ =>
        // For all other primitive types, return as-is
        value
    }
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
      case TimestampType =>
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