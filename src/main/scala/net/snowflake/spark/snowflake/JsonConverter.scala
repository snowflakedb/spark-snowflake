package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.Conversions.jsonStringToRow

import scala.reflect.ClassTag

object JsonConverter {

  private val mapper: ObjectMapper = new ObjectMapper()

  private[snowflake] def convert[T: ClassTag](
    partition: Iterator[String],
    resultSchema: StructType,
  ): Iterator[T] =
    partition.map(convertRow[T](resultSchema, _))

  private[snowflake] def convertRow[T: ClassTag](schema: StructType,
                                                 fields: String): T = {
    val json = mapper.readTree(fields)
    jsonStringToRow[T](json, schema).asInstanceOf[T]
  }

}
