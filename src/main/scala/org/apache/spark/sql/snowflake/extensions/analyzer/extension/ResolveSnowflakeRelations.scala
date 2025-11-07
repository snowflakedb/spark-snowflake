package org.apache.spark.sql.snowflake.extensions.analyzer.extension

import net.snowflake.spark.snowflake.DefaultSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.snowflake.catalog.FGACForbiddenException
import org.slf4j.LoggerFactory

case class ResolveSnowflakeRelations(
    spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  protected val logger = LoggerFactory.getLogger(getClass)
  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager
  private val snowflakeSource = new DefaultSource()
  
  private val FGAC_JDBC_FALLBACK_ENABLED_KEY = "spark.snowflake.extensions.fgacJdbcFallback.enabled"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val fgacJdbcFallbackEnabled =
        spark.sessionState.conf.getConfString(FGAC_JDBC_FALLBACK_ENABLED_KEY, "false").toBoolean
    
    if (!fgacJdbcFallbackEnabled) {
      return plan
    }
    
    plan transformUp {
      case u: UnresolvedRelation =>
        u.multipartIdentifier match {
          case parts @ CatalogAndIdentifier(catalog, ident) =>
            if (shouldFallbackToSnowflake(catalog, ident)) {
              logger.debug("Resolving {} via Snowflake JDBC fallback", parts.mkString("."))
              createSnowflakeRelation(parts)
            } else {
              u
            }
          case _ => u
        }
      
      case i: InsertIntoStatement =>
        i.table match {
          case u: UnresolvedRelation =>
            u.multipartIdentifier match {
              case parts @ CatalogAndIdentifier(catalog, ident) =>
                if (shouldFallbackToSnowflake(catalog, ident)) {
                  logger.debug(
                      "Resolving INSERT target {} via Snowflake JDBC fallback", parts.mkString("."))
                  val resolvedTable = createSnowflakeRelation(parts)
                  i.copy(table = resolvedTable)
                } else {
                  i
                }
              case _ => i
            }
          case _ => i
        }
    }
  }
  
  private def shouldFallbackToSnowflake(
      catalog: org.apache.spark.sql.connector.catalog.CatalogPlugin,
      ident: org.apache.spark.sql.connector.catalog.Identifier): Boolean = {
    try {
      catalog.asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog].loadTable(ident)
      false
    } catch {
      case _: FGACForbiddenException => true
      case _: NoSuchTableException => false
      case _: Throwable => false
    }
  }

  private def createSnowflakeRelation(nameParts: Seq[String]): LogicalPlan = {
    try {
      val tableName = nameParts.last
      val schemaName = if (nameParts.length > 1) Some(nameParts(nameParts.length - 2)) else None
      
      val options = buildSnowflakeOptions(schemaName, tableName)
      val baseRelation = snowflakeSource.createRelation(spark.sqlContext, options)
      
      logger.info("Created Snowflake JDBC relation for table: {}", nameParts.mkString("."))
      LogicalRelation(baseRelation, isStreaming = false)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create Snowflake relation for ${nameParts.mkString(".")}", ex)
        throw new RuntimeException(
          s"Failed to create Snowflake relation for ${nameParts.mkString(".")}", ex)
    }
  }

  private def buildSnowflakeOptions(
    schemaName: Option[String], tableName: String): Map[String, String] = {
    val fullTableName = schemaName match {
      case Some(schema) => s"$schema.$tableName"
      case None => tableName
    }
    
    val baseOptions = Map("dbtable" -> fullTableName)
    
    val conf = spark.sessionState.conf
    val snowflakeOptions = conf.getAllConfs.filter { case (key, _) =>
      key.toLowerCase.startsWith("spark.snowflake.") || key.toLowerCase.startsWith("snowflake.")
    }.map { case (key, value) =>
      val cleanKey = if (key.toLowerCase.startsWith("spark.snowflake.")) {
        key.substring("spark.snowflake.".length)
      } else if (key.toLowerCase.startsWith("snowflake.")) {
        key.substring("snowflake.".length)
      } else {
        key
      }
      cleanKey -> value
    }
    
    baseOptions ++ snowflakeOptions
  }
}
