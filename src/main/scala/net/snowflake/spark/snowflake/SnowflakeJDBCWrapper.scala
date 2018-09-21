/*
 * Copyright 2015-2018 Snowflake Computing
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

package net.snowflake.spark.snowflake

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException, Statement}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}

import net.snowflake.client.jdbc.telemetry.Telemetry
import net.snowflake.client.jdbc.telemetry.Telemetry.createTelemetry

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.Utils.JDBC_DRIVER
import DefaultJDBCWrapper.DataBaseOperations

/**
  * Shim which exposes some JDBC helper functions. Most of this code is copied from Spark SQL, with
  * minor modifications for Snowflake-specific features and limitations.
  */
private[snowflake] class JDBCWrapper {

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

  /**
    * Takes a (schema, table) specification and returns the table's Catalyst
    * schema.
    *
    * @param conn  A JDBC connection to the database.
    * @param table The table name of the desired table.  This may also be a
    *              SQL query wrapped in parentheses.
    * @return A StructType giving the table's Catalyst schema.
    * @throws SQLException if the table specification is garbage.
    * @throws SQLException if the table contains an unsupported type.
    */
  def resolveTable(conn: Connection, table: String): StructType =
    resolveTableFromMeta(conn, conn.tableMetaData(table))

  def resolveTableFromMeta(conn: Connection, rsmd: ResultSetMetaData): StructType = {
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
      fields(i) = StructField(
        // Add quotes around column names if Snowflake would usually require them.
        if (columnName.matches("[_A-Z]([_0-9A-Z])*"))
          columnName
        else
          s""""$columnName"""",
        columnType,
        nullable)
      i = i + 1
    }
    new StructType(fields)
  }

  /**
    * Get a connection based on the provided parameters
    */
  def getConnector(params: MergedParameters): Connection = {
    // Derive class name
    val driverClassName = JDBC_DRIVER
    try {
      val driverClass = Utils.classForName(driverClassName)
      DriverRegistry.register(driverClass.getCanonicalName)
    } catch {
      case e: ClassNotFoundException =>
        throw new ClassNotFoundException(s"Could not load a Snowflake JDBC driver class < $driverClassName > ", e)
    }

    val sfURL = params.sfURL
    val jdbcURL = s"""jdbc:snowflake://$sfURL"""

    val jdbcProperties = new Properties()

    // Obligatory properties
    jdbcProperties.put("db", params.sfDatabase)
    jdbcProperties.put("schema", params.sfSchema) // Has a default
    jdbcProperties.put("user", params.sfUser)
    jdbcProperties.put("password", params.sfPassword)
    jdbcProperties.put("ssl", params.sfSSL) // Has a default
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
    jdbcProperties.put("client_session_keep_alive", "true")

    // Force DECIMAL for NUMBER (SNOW-33227)
    jdbcProperties.put("JDBC_TREAT_DECIMAL_AS_INT", "false")

    // Add extra properties from sfOptions
    val extraOptions = params.sfExtraOptions
    for ((k: String, v: Object) <- extraOptions) {
      jdbcProperties.put(k.toLowerCase, v.toString())
    }

    // Set info on the system level
    // Very simple escaping
    def esc(s: String): String = {
      s.replace("\"", "").replace("\\", "")
    }

    val sparkAppName = SparkContext.getOrCreate().getConf.get("spark.app.name", "")
    val scalaVersion = util.Properties.versionString
    val javaVersion = System.getProperty("java.version", "UNKNOWN")
    val snowflakeClientInfo =
      s""" {
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
    schema.fields.map(field => {
      val name: String = Utils.ensureQuoted(field.name)
      val `type`: String = schemaConversion(field)
      val nullable: String = if (field.nullable) "" else "NOT NULL"
      s"""$name ${`type`} $nullable"""
    }).mkString(",")
  }

  /**
    * Retrieve corresponding data type in snowflake of giving struct field
    */
  def schemaConversion(field: StructField): String =
    field.dataType match {
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
      case _: StructType | _: ArrayType | _: MapType => "VARIANT"
      case _ =>
        throw new IllegalArgumentException(s"Don't know how to save $field of type ${field.name} to Snowflake")
    }


  /**
    * Returns true if the table already exists in the JDBC database.
    */
  def tableExists(conn: Connection, table: String): Boolean = conn.tableExists(table)

  // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
  // SQL database systems, considering "table" could also include the database name.

  def executePreparedInterruptibly(statement: PreparedStatement): Boolean = {
    executeInterruptibly(statement,
      {
        stmt: Statement =>
          val prepStmt = stmt.asInstanceOf[PreparedStatement]
          prepStmt.execute()
      })
  }

  def executePreparedInterruptibly(conn: Connection, sql: String): Boolean = {
    executePreparedInterruptibly(conn.prepareStatement(sql))
  }

  def executePreparedQueryInterruptibly(statement: PreparedStatement): ResultSet = {
    executeInterruptibly(statement,
      {
        stmt: Statement =>
          val prepStmt = stmt.asInstanceOf[PreparedStatement]
          prepStmt.executeQuery()
      })
  }

  def executePreparedQueryInterruptibly(conn: Connection, sql: String): ResultSet = {
    executePreparedQueryInterruptibly(conn.prepareStatement(sql))
  }

  def executeQueryInterruptibly(statement: Statement, str: String): ResultSet = {
    executeInterruptibly(statement, _.executeQuery(str))
  }

  def executeInterruptibly(statement: Statement, str: String): Boolean = {
    executeInterruptibly(statement, _.execute(str))
  }

  /**
    * A version of <code>executeQueryInterruptibly</code> accepting a string
    */
  def executeQueryInterruptibly(conn: Connection, sql: String): ResultSet = {
    val stmt = conn.createStatement
    executeQueryInterruptibly(stmt, sql)
  }

  /**
    * A version of <code>executeQueryInterruptibly</code> accepting a string
    */
  def executeInterruptibly(conn: Connection, sql: String): Boolean = {
    val stmt = conn.createStatement
    executeInterruptibly(stmt, sql)
  }

  private def executeInterruptibly[T](statement: Statement, op: Statement => T): T = {
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
  private def getCatalystType(sqlType: Int, precision: Int, scale: Int, signed: Boolean): DataType = {
    // TODO: cleanup types which are irrelevant for Snowflake.
    // Snowflake-todo: Add support for some types like ARRAY.
    // Snowflake-todo: Verify all types.
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT => if (signed) {
        LongType
      } else {
        DecimalType(20, 0)
      }
      //      case java.sql.Types.BINARY        => BinaryType
      //      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      //      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 => {
        if (precision > DecimalType.MAX_PRECISION) {
          DecimalType(DecimalType.MAX_PRECISION, scale + (precision - DecimalType.MAX_SCALE))
        } else {
          DecimalType(precision, scale)
        }
      }
      case java.sql.Types.DECIMAL => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) {
        IntegerType
      } else {
        LongType
      }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      //      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType // Snowflake-todo: ?
      case java.sql.Types.STRUCT => StringType // Snowflake-todo: ?
      //      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TINYINT => IntegerType
      //      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }

  @deprecated
  def getTelemetry(conn: Connection): Telemetry = createTelemetry(conn)
}

private[snowflake] object DefaultJDBCWrapper extends JDBCWrapper {

  implicit class DataBaseOperations(connection: Connection) {

    /**
      * @return telemetry connector
      */
    def getTelemetry: Telemetry = createTelemetry(connection)

    /**
      * Create a table
      *
      * @param name      table name
      * @param schema    table schema
      * @param overwrite use "create or replace" if true,
      *                  otherwise, use "create if not exists"
      */
    def createTable(
                     name: String,
                     schema: StructType,
                     overwrite: Boolean = false,
                     temporary: Boolean = false
                   ): Unit =
      (ConstantString("create") +
        (if (overwrite) "or replace" else "") +
        (if (temporary) "temporary" else "") + "table" +
        (if (!overwrite) "if not exists" else "") + Identifier(name) +
        s"(${schemaString(schema)})").execute(connection)

    def createTableLike(
                         newTable: String,
                         originalTable: String
                       ): Unit = {
      (ConstantString("create or replace table") + Identifier(newTable) +
        "like" + Identifier(originalTable)).execute(connection)
    }

    def truncateTable(table: String): Unit =
      (ConstantString("truncate") + table).execute(connection)

    def swapTable(
                   newTable: String,
                   originalTable: String
                 ): Unit =
      (ConstantString("alter table") + Identifier(newTable) + "swap with" +
        Identifier(originalTable)).execute(connection)

    def renameTable(
                     newName: String,
                     oldName: String
                   ): Unit =
      (ConstantString("alter table") + Identifier(oldName) + "rename to" +
        Identifier(newName)).execute(connection)

    /**
      * @param name table name
      * @return true if table exists, otherwise false
      */
    def tableExists(name: String): Boolean =
      Try {
        (EmptySnowflakeSQLStatement() + "desc table" + Identifier(name)).execute(connection)
      }.isSuccess

    /**
      * Drop a table
      *
      * @param name table name
      * @return true if table dropped, false if the given table not exists
      */
    def dropTable(name: String): Boolean =
      Try {
        (EmptySnowflakeSQLStatement() + "drop table" + Identifier(name)).execute(connection)
      }.isSuccess

    def tableMetaData(name: String): ResultSetMetaData =
      try {
        tableMetaDataFromStatement(Identifier(name) !)
      } catch {
        case _: Exception =>
          tableMetaDataFromStatement(ConstantString(name) !)
      }

    def tableMetaDataFromStatement(statement: SnowflakeSQLStatement): ResultSetMetaData =
      (ConstantString("select * from") + statement + "where 1 = 0")
        .execute(connection).getMetaData

    def tableSchema(name: String): StructType = resolveTable(connection, name)

    def tableSchema(statement: SnowflakeSQLStatement): StructType =
      resolveTableFromMeta(connection, tableMetaDataFromStatement(statement))

    /**
      * Create an internal stage if location is None,
      * otherwise create an external stage
      *
      * @param name         stage name
      * @param location     storage path for external stage
      * @param awsAccessKey aws access key
      * @param awsSecretKey aws secret key
      * @param azureSAS     azure sas
      * @param overwrite    use "create or replace stage" if true,
      *                     otherwise use " create stage if not exists"
      * @param temporary    create temporary stage if it is true
      */
    def createStage(
                     name: String,
                     location: Option[String] = None,
                     awsAccessKey: Option[String] = None,
                     awsSecretKey: Option[String] = None,
                     azureSAS: Option[String] = None,
                     overwrite: Boolean = false,
                     temporary: Boolean = false
                   ): Unit = {

      (ConstantString("create") +
        (if (overwrite) "or replace" else "") +
        (if (temporary) "temporary" else "") + "stage" +
        (if (!overwrite) "if not exists" else "") + Identifier(name) +
        (location match {
          case Some(path) =>
            ConstantString(s"url='$path' credentials = (") +
              (azureSAS match {
                case Some(sas) =>
                  ConstantString(s"azure_sas_token = '$sas')")
                case None =>
                  ConstantString(s"aws_key_id = '${awsAccessKey.get}' aws_secret_key = '${awsSecretKey.get}')")
              })
          case None => EmptySnowflakeSQLStatement()
        })
        ).execute(connection)

    }

    /**
      * Drop a stage
      *
      * @param name stage name
      * @return true if stage dropped, false if the given stage not exists.
      */
    def dropStage(name: String): Boolean =
      Try {
        (ConstantString("drop stage") + Identifier(name)).execute(connection)
      }.isSuccess


    /**
      *
      * @param name stage name
      * @return true is stage exists, otherwise false
      */
    def stageExists(name: String): Boolean = {
      Try {
        (EmptySnowflakeSQLStatement() + "desc stage" + Identifier(name)).execute(connection)
      }.isSuccess
    }

    def execute(statement: SnowflakeSQLStatement): Unit =
      statement.execute(connection)

    def copyFromSnowflake(): Unit = {}

    def copyToSnowflake(): Unit = {}

    //pipe operations
    def createPipe(name: String): Unit = throw new NotImplementedError()

    def dropPipe(name: String): Boolean = throw new NotImplementedError()

    def pipeExists(name: String): Boolean = throw new NotImplementedError()
  }


}

/**
  * SQL string wrapper
  */
private[snowflake] class SnowflakeSQLStatement(
                                                val numOfVar: Int = 0,
                                                val list: List[StatementElement] = Nil
                                              ) {

  def +(element: StatementElement): SnowflakeSQLStatement =
    new SnowflakeSQLStatement(
      numOfVar + element.isVariable,
      element :: list
    )

  def +(statement: SnowflakeSQLStatement): SnowflakeSQLStatement =
    new SnowflakeSQLStatement(
      numOfVar + statement.numOfVar,
      statement.list ::: list
    )

  def +(str: String): SnowflakeSQLStatement = this + ConstantString(str)

  def isEmpty: Boolean = list.isEmpty

  def execute(implicit conn: Connection): ResultSet = {
    val sql = list.reverse
    val varArray: Array[StatementElement] = new Array[StatementElement](numOfVar)
    var indexOfVar: Int = 0
    val buffer = new StringBuilder

    sql.foreach(element => {
      buffer.append(element)
      if (!element.isInstanceOf[ConstantString]) {
        varArray(indexOfVar) = element
        indexOfVar += 1
      }
      buffer.append(" ")
    })
    val statement = conn.prepareStatement(buffer.toString())
    varArray.zipWithIndex.foreach {
      case (element, index) => {
        element match {
          case ele: StringVariable =>
            statement.setString(index + 1, ele.variable)
          case ele: Identifier =>
            statement.setString(index + 1, ele.variable)
          case ele: IntVariable =>
            statement.setInt(index + 1, ele.variable)
          case ele: LongVariable =>
            statement.setLong(index + 1, ele.variable)
          case ele: ShortVariable =>
            statement.setShort(index + 1, ele.variable)
          case ele: FloatVariable =>
            statement.setFloat(index + 1, ele.variable)
          case ele: DoubleVariable =>
            statement.setDouble(index + 1, ele.variable)
          case ele: BooleanVariable =>
            statement.setBoolean(index + 1, ele.variable)
          case ele: ByteVariable =>
            statement.setByte(index + 1, ele.variable)
          case _ =>
            throw new IllegalArgumentException("Unexpected Element Type: " + element.getClass.getName)
        }
      }
    }

    DefaultJDBCWrapper.executePreparedQueryInterruptibly(statement)


  }

  override def equals(obj: scala.Any): Boolean =
    obj match {
      case other: SnowflakeSQLStatement =>
        if (this.statementString == other.statementString) true else false
      case _ => false
    }


  override def toString: String = {
    val buffer = new StringBuilder
    val sql = list.reverse

    sql.foreach {
      case x: ConstantString => {
        if (buffer.nonEmpty && buffer.last != ' ') buffer.append(" ")
        buffer.append(x)
      }
      case x: VariableElement[_] => {
        if (buffer.nonEmpty && buffer.last != ' ') buffer.append(" ")
        buffer.append(x.sql)
      }
    }

    buffer.toString()
  }

  def statementString: String = {
    val buffer = new StringBuilder
    val sql = list.reverse

    sql.foreach {
      case x: ConstantString => {
        if (buffer.nonEmpty && buffer.last != ' ') buffer.append(" ")
        buffer.append(x)
      }
      case x: VariableElement[_] => {
        if (buffer.nonEmpty && buffer.last != ' ') buffer.append(" ")
        buffer.append(x.value)
        buffer.append("(")
        buffer.append(x.variable)
        buffer.append(")")
      }
    }

    buffer.toString()
  }

}

private[snowflake] object EmptySnowflakeSQLStatement {
  def apply(): SnowflakeSQLStatement = new SnowflakeSQLStatement()
}

private[snowflake] sealed trait StatementElement {

  val value: String

  val isVariable: Int = 0

  def +(element: StatementElement): SnowflakeSQLStatement =
    new SnowflakeSQLStatement(
      isVariable + element.isVariable,
      element :: List[StatementElement](this)
    )

  def +(statement: SnowflakeSQLStatement): SnowflakeSQLStatement =
    new SnowflakeSQLStatement(
      isVariable + statement.numOfVar,
      statement.list ::: List[StatementElement](this)
    )

  def +(str: String): SnowflakeSQLStatement = this + ConstantString(str)

  override def toString: String = value

  def ! : SnowflakeSQLStatement = toStatement

  def toStatement: SnowflakeSQLStatement =
    new SnowflakeSQLStatement(isVariable, List[StatementElement](this))
}

private[snowflake] case class ConstantString(override val value: String) extends StatementElement

private[snowflake] sealed trait VariableElement[T] extends StatementElement {
  override val value = "?"

  override val isVariable: Int = 1

  val variable: T

  def sql: String = variable.toString

}

private[snowflake] case class Identifier(override val variable: String) extends VariableElement[String] {
  override val value: String = "identifier(?)"
}

private[snowflake] case class StringVariable(override val variable: String) extends VariableElement[String] {
  override def sql: String = s"""'$variable'"""
}

private[snowflake] case class IntVariable(override val variable: Int) extends VariableElement[Int]

private[snowflake] case class LongVariable(override val variable: Long) extends VariableElement[Long]

private[snowflake] case class ShortVariable(override val variable: Short) extends VariableElement[Short]

private[snowflake] case class FloatVariable(override val variable: Float) extends VariableElement[Float]

private[snowflake] case class DoubleVariable(override val variable: Double) extends VariableElement[Double]

private[snowflake] case class BooleanVariable(override val variable: Boolean) extends VariableElement[Boolean]

private[snowflake] case class ByteVariable(override val variable: Byte) extends VariableElement[Byte]

