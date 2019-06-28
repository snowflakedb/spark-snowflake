package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Parameters.MergedParameters

package object pushdowns {
  //let pushdown function access parameters
  var globalParameter: Option[MergedParameters] = None

  def setGlobalParameter(param: MergedParameters): Unit = {
    globalParameter = Option(param)
  }
}
