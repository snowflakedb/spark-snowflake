/*
 * Copyright 2015-2016 Snowflake Computing
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowflakedb.spark.snowflakedb

import java.net.URI
import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import com.snowflakedb.spark.snowflakedb.Parameters.MergedParameters

/**
 * Shim which exposes some JDBC helper functions. Most of this code is copied from Spark SQL, with
 * minor modifications for Snowflake-specific features and limitations.
 */
private[snowflakedb] class JDBCWrapper {

  private val log = LoggerFactory.getLogger(getClass)

  private val SPARK_SNOWFLAKEDB_VERSION = Option(getClass.getPackage.getImplementationVersion).getOrElse("<UNKNOWN>")

  private val ec: ExecutionContext = {
    log.debug("Creating a new ExecutionContext")
    val threadFactory = new ThreadFactory {
      private[this] val count = new AtomicInteger()
      override def newThread(r: Runnable) = {
        val thread = new Thread(r)
        thread.setName(s"spark-snowflake-JDBCWrapper-${count.incrementAndGet}")
        thread.setDaemon(true)
        thread
      }
    }
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool(threadFactory))
  }

  private def registerDriver(driverClass: String): Unit = {
    // DriverRegistry.register() is one of the few pieces of private Spark functionality which
    // we need to rely on. This class was relocated in Spark 1.5.0, so we need to use reflection
    // in order to support both Spark 1.4.x and 1.5.x.
    if (SPARK_VERSION.startsWith("1.4")) {
      val className = "org.apache.spark.sql.jdbc.package$DriverRegistry$"
      val driverRegistryClass = Utils.classForName(className)
      val registerMethod = driverRegistryClass.getDeclaredMethod("register", classOf[String])
      val companionObject = driverRegistryClass.getDeclaredField("MODULE$").get(null)
      registerMethod.invoke(companionObject, driverClass)
    } else { // Spark 1.5.0+
      val className = "org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry"
      val driverRegistryClass = Utils.classForName(className)
      val registerMethod = driverRegistryClass.getDeclaredMethod("register", classOf[String])
      registerMethod.invoke(null, driverClass)
    }
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst
   * schema.
   *
   * @param conn A JDBC connection to the database.
   * @param table The table name of the desired table.  This may also be a
   *   SQL query wrapped in parentheses.
   * @return A StructType giving the table's Catalyst schema.
   * @throws SQLException if the table specification is garbage.
   * @throws SQLException if the table contains an unsupported type.
   */
  def resolveTable(conn: Connection, table: String): StructType = {
    val rs = executeQueryInterruptibly(conn, s"SELECT * FROM $table WHERE 1=0")
    try {
      val rsmd = rs.getMetaData
      val ncols = rsmd.getColumnCount
      val fields = new Array[StructField](ncols)
      var i = 0
      while (i < ncols) {
        val columnName = rsmd.getColumnLabel(i + 1)
        val dataType = rsmd.getColumnType(i + 1)
        val typeName = rsmd.getColumnTypeName(i + 1)
        val fieldSize = rsmd.getPrecision(i + 1)
        val fieldScale = rsmd.getScale(i + 1)
        val isSigned = rsmd.isSigned(i + 1)
        val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
        val columnType = getCatalystType(dataType, fieldSize, fieldScale, isSigned)
        fields(i) = StructField(columnName, columnType, nullable)
        i = i + 1
      }
      new StructType(fields)
    } finally {
      rs.close()
    }
  }


  /**
   *  Get a connection based on the provided parameters
   */
  def getConnector(params: MergedParameters): Connection = {
    // Derive class name
    val driverClassName = params.jdbcDriver.
      getOrElse("com.snowflake.client.jdbc.SnowflakeDriver")
    try {
      val driverClass = Utils.classForName(driverClassName)
      registerDriver(driverClass.getCanonicalName)
    } catch {
      case e: ClassNotFoundException =>
        throw new ClassNotFoundException(
          s"Could not load a Snowflake JDBC driver class < $driverClassName > ", e)
    }

    val sfURL = params.sfURL
    val jdbcURL = s"""jdbc:snowflake://$sfURL"""

    val jdbcProperties = new Properties()

    // Obligatory properties
    jdbcProperties.put("db", params.sfDatabase)
    jdbcProperties.put("schema", params.sfSchema)  // Has a default
    jdbcProperties.put("user", params.sfUser)
    jdbcProperties.put("password", params.sfPassword)
    jdbcProperties.put("ssl", params.sfSSL)  // Has a default
    // Optional properties
    if (params.sfAccount.isDefined) {
      jdbcProperties.put("account", params.sfAccount.get)
    }
    if (params.sfWarehouse.isDefined) {
      jdbcProperties.put("warehouse", params.sfWarehouse.get)
    }
    if (params.sfRole.isDefined) {
      jdbcProperties.put("role", params.sfRole.get)
    }

    // Always set CLIENT_SESSION_KEEP_ALIVE.
    // Note, can be overridden with options
    jdbcProperties.put("client_session_keep_alive", new java.lang.Boolean(true))

    // Add extra properties from sfOptions
    val extraOptions = params.sfExtraOptions
    for ((k: String, v: Object) <- extraOptions) {
      jdbcProperties.put(k.toLowerCase, v)
    }

    // Set info on the system level
    // Very simple escaping
    def esc(s: String) : String = {
      s.replace("\"", "").replace("\\", "")
    }
    val sparkAppName = SparkContext.getOrCreate().getConf.get("spark.app.name", "")
    val scalaVersion = scala.tools.nsc.Properties.versionString
    val javaVersion = System.getProperty("java.version", "UNKNOWN")
    val snowflakeClientInfo = s""" {
        | "spark.version" : "${esc(SPARK_VERSION)}",
        | "spark.snowflakedb.version" : "${esc(SPARK_SNOWFLAKEDB_VERSION)}",
        | "spark.app.name" : "${esc(sparkAppName)}",
        | "scala.version" : "${esc(scalaVersion)}",
        | "java.version" : "${esc(javaVersion)}"
        |}""".stripMargin
    log.debug(snowflakeClientInfo)
    System.setProperty("snowflake.client.info", snowflakeClientInfo)

    var conn = DriverManager.getConnection(jdbcURL, jdbcProperties)

    // Set info on the connection level
    conn.setClientInfo("spark-version", SPARK_VERSION)
    conn.setClientInfo("spark-snowflakedb-version", SPARK_SNOWFLAKEDB_VERSION)
    log.debug(conn.getClientInfo.toString)

    conn
  }


  /**
   * Compute the SQL schema string for the given Spark SQL Schema.
   */
  def schemaString(schema: StructType): String = {
    val sb = new StringBuilder()
    schema.fields.foreach { field => {
      val name = field.name
      val typ: String = field.dataType match {
        case IntegerType => "INTEGER"
        case LongType => "INTEGER"
        case DoubleType => "DOUBLE"
        case FloatType => "FLOAT"
        case ShortType => "INTEGER"
        case ByteType => "INTEGER" // Snowflake does not support the BYTE type.
        case BooleanType => "BOOLEAN"
        case StringType =>
          if (field.metadata.contains("maxlength")) {
            s"VARCHAR(${field.metadata.getLong("maxlength")})"
          } else {
            "STRING"
          }
//        case BinaryType => "BLOB"
        case TimestampType => "TIMESTAMP"
        case DateType => "DATE"
        case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
        case _ => throw new IllegalArgumentException(s"Don't know how to save $field of type ${field.name} to Snowflake")
      }
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s""", "${name.replace("\"", "\\\"")}" $typ $nullable""".trim)
    }}
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, table: String): Boolean = {
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems, considering "table" could also include the database name.
    Try {
      executeQueryInterruptibly(conn, s"SELECT 1 FROM $table LIMIT 1").next()
    }.isSuccess
  }

  /**
    * Execute the given SQL statement while supporting interruption.
    * If InterruptedException is caught, then the statement will be cancelled if it is running.
    *
    * @return <code>true</code> if the first result is a <code>ResultSet</code>
    *         object; <code>false</code> if the first result is an update
    *         count or there is no result
    */
  def executeInterruptibly(statement: PreparedStatement): Boolean = {
    executeInterruptibly(statement, _.execute())
  }

  /**
    * A version of <code>executeInterruptibly</code> accepting a string
    */
  def executeInterruptibly(conn: Connection, sql: String): Boolean = {
    executeInterruptibly(conn.prepareStatement(sql))
  }

  /**
    * Execute the given SQL statement while supporting interruption.
    * If InterruptedException is caught, then the statement will be cancelled if it is running.
    *
    * @return a <code>ResultSet</code> object that contains the data produced by the
    *         query; never <code>null</code>
    */
  def executeQueryInterruptibly(statement: PreparedStatement): ResultSet = {
    executeInterruptibly(statement, _.executeQuery())
  }

  /**
    * A version of <code>executeQueryInterruptibly</code> accepting a string
    */
  def executeQueryInterruptibly(conn: Connection, sql: String): ResultSet = {
    executeQueryInterruptibly(conn.prepareStatement(sql))
  }

  private def executeInterruptibly[T](
                                       statement: PreparedStatement,
                                       op: PreparedStatement => T): T = {
    try {
      log.debug(s"Running statement $statement")
      val future = Future[T](op(statement))(ec)
      Await.result(future, Duration.Inf)
    } catch {
      case e: InterruptedException =>
        try {
          log.info(s"Cancelling statement $statement")
          statement.cancel()
          log.info("Cancelling succeeded")
          throw e
        } catch {
          case s: SQLException =>
            log.error("Exception occurred while cancelling query", s)
            throw e
        }
    }
  }

  /**
   * Maps a JDBC type to a Catalyst type.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
  private def getCatalystType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType = {
    // TODO: cleanup types which are irrelevant for Snowflake.
    // Snowflake-todo: Add support for some types like ARRAY.
    // Snowflake-todo: Verify all types.
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
//      case java.sql.Types.BINARY        => BinaryType
//      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
//      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
//      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType // Snowflake-todo: ?
      case java.sql.Types.STRUCT        => StringType // Snowflake-todo: ?
//      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TINYINT       => IntegerType
//      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }
}

private[snowflakedb] object DefaultJDBCWrapper extends JDBCWrapper
