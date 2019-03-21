package net.snowflake.spark.snowflake_beta

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake_beta.Conversions.jsonStringToRow

import scala.reflect.ClassTag

object JsonConverter {

  private val mapper: ObjectMapper = new ObjectMapper()

  private[snowflake_beta] def convert[T: ClassTag](
                                              partition: Iterator[String],
                                              resultSchema: StructType
                                             ): Iterator[T] =
    partition.map(convertRow[T](resultSchema,_))



  private[snowflake_beta] def convertRow[T:ClassTag](
                                                 schema: StructType,
                                                 fields: String): T = {
    val json = mapper.readTree(fields)
    jsonStringToRow[T](json, schema).asInstanceOf[T]
  }



}
