package net.snowflake.spark.snowflake_beta

import java.sql.Connection

import net.snowflake.spark.snowflake_beta.Parameters.MergedParameters
import net.snowflake.spark.snowflake_beta.io.{CloudStorage, SupportedFormat}
import net.snowflake.spark.snowflake_beta.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake_beta.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

package object streaming {

  private val LOGGER = LoggerFactory.getLogger(this.getClass.getName)
  private val SLEEP_TIME = 5000 // 5 seconds
  private val TIME_OUT = 5 // 5 minutes

  private val pipeList: mutable.HashMap[String, SnowflakeIngestService] =
    new mutable.HashMap()

  private[streaming] def openIngestionService(
                               param: MergedParameters,
                               pipeName: String,
                               format: SupportedFormat,
                               schema: StructType,
                               storage: CloudStorage,
                               conn: Connection
                             ): SnowflakeIngestService = {
    LOGGER.debug(s"create new ingestion service, pipe name: $pipeName")

    var pipeDropped = false
    val checkPrevious: Future[Boolean] = Future{
      while(pipeList.contains(pipeName)) {
        LOGGER.debug(s"waiting previous pipe dropped")
        Thread.sleep(SLEEP_TIME)
      }
      LOGGER.debug(s"previous pipe dropped")
      pipeDropped = true
      pipeDropped
    }

    Await.result(checkPrevious, TIME_OUT minutes)

    if(pipeDropped) {
      conn.createTable(param.table.get.name, schema, param, overwrite = false)

      val copy = ConstantString(copySql(param, conn, format)) !

      if(verifyPipe(conn, pipeName, copy.toString)) {
        LOGGER.info(s"reuse pipe: $pipeName")
      } else conn.createPipe(pipeName, copy, true)

      val ingestion = new SnowflakeIngestService(param, pipeName, storage, conn)
      pipeList.put(
        pipeName,
        ingestion
      )
      ingestion
    } else {
      LOGGER.error(s"waiting pipe dropped time out")
      throw
        new IllegalStateException(s"Waiting pipe dropped time out, pipe name: $pipeName")
    }
  }

  private[streaming] def closeIngestionService(pipeName: String): Unit = {
    LOGGER.debug(s"closing ingestion service, pipe name: $pipeName")
    if(pipeList.contains(pipeName)){
      pipeList(pipeName).close()
      pipeList.remove(pipeName)
      LOGGER.debug(s"ingestion service closed, pipe name: $pipeName")
    } else{
      LOGGER.error(s"ingestion service not found, pipe name: $pipeName")
    }
  }

  private[streaming] def closeAllIngestionService(): Unit = {
    LOGGER.debug(s"closing ingestion service")
    pipeList.par.foreach(_._2.close())
    LOGGER.debug(s"all ingestion service closed")
  }


  /**
    * Generate the COPY SQL command for creating pipe only
    */
  private def copySql(
                       param: MergedParameters,
                       conn: Connection,
                       format: SupportedFormat
                     ): String = {

    val tableName = param.table.get
    val stageName = param.streamingStage.get
    val tableSchema = DefaultJDBCWrapper.resolveTable(conn, tableName.toString, param)

    def getMappingToString(list: Option[List[(Int, String)]]): String =
      format match {
        case SupportedFormat.JSON =>
          val schema = DefaultJDBCWrapper.resolveTable(conn, tableName.name, param)
          if (list.isEmpty || list.get.isEmpty)
            s"(${schema.fields.map(x => Utils.ensureQuoted(x.name)).mkString(",")})"
          else s"(${list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ")})"
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) ""
          else s"(${list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ")})"
      }

    def getMappingFromString(list: Option[List[(Int, String)]], from: String): String =
      format match {
        case SupportedFormat.JSON =>
          if (list.isEmpty || list.get.isEmpty) {
            val names =
              tableSchema
                .fields
                .map(x => "parse_json($1):".concat(Utils.ensureQuoted(x.name)))
                .mkString(",")
            s"from (select $names $from tmp)"
          }
          else
            s"from (select ${list.get.map(x => "parse_json($1):".concat(Utils.ensureQuoted(tableSchema(x._1 - 1).name))).mkString(", ")} $from tmp)"
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) from
          else
            s"from (select ${list.get.map(x => "tmp.$".concat(Utils.ensureQuoted(x._1.toString))).mkString(", ")} $from tmp)"
      }

    val fromString = s"FROM @$stageName"

    val mappingList: Option[List[(Int, String)]] = param.columnMap match {
      case Some(map) =>
        Some(map.toList.map {
          case (key, value) =>
            try {
              (tableSchema.fieldIndex(key) + 1, value)
            } catch {
              case e: Exception => {
                LOGGER.error("Error occurred while column mapping: " + e)
                throw e
              }
            }
        })

      case None => None
    }

    val mappingToString = getMappingToString(mappingList)

    val mappingFromString = getMappingFromString(mappingList, fromString)

    val formatString =
      format match {
        case SupportedFormat.CSV =>
          s"""
             |FILE_FORMAT = (
             |    TYPE=CSV
             |    FIELD_DELIMITER='|'
             |    NULL_IF=()
             |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
             |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
             |  )
           """.stripMargin
        case SupportedFormat.JSON =>
          s"""
             |FILE_FORMAT = (
             |    TYPE = JSON
             |)
           """.stripMargin
      }

    s"""
       |COPY INTO $tableName $mappingToString
       |$mappingFromString
       |$formatString
    """.stripMargin.trim
  }

  private[streaming] def verifyPipe(
                                     conn: Connection,
                                     pipeName: String,
                                     copyStatement: String
                                   ): Boolean =
    conn.pipeDefinition(pipeName) match {
      case Some(str) => str.trim.equals(copyStatement.trim)
      case _ => false
    }



}
