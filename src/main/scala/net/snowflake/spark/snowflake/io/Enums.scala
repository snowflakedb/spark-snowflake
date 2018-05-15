package net.snowflake.spark.snowflake.io

object SupportedSource extends Enumeration {
  type SupportedSource = Value
  val S3INTERNAL, S3EXTERNAL = Value

}

object SupportedFormat extends Enumeration {
  type SupportedFormat = Value
  val CSV, JSON = Value
}
