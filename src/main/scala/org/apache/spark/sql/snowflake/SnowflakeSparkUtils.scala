package org.apache.spark.sql.snowflake

import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRelation, JdbcRelationProvider}

object SnowflakeSparkUtils {

  private def getClassName(obj: Any): String = {
    val shortClassNames = Seq("net.snowflake.spark.snowflake.SnowflakeRelation",
      "net.snowflake.spark.snowflake.DefaultSource",
      "org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider",
      "org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation")
    val className = obj.getClass.getName
    if (shortClassNames.contains(className)) {
      className.split("\\.").last
    } else {
      className
    }
  }

  private[sql] def getJDBCProviderName(url: String): String = {
    if (url != null && url.startsWith("jdbc:")) {
      val parts = url.split(":")
      if (parts.length > 2 && parts(1).nonEmpty) {
        parts(1).toLowerCase
      } else {
        "unknown"
      }
    } else {
      "unknown"
    }
  }
}
