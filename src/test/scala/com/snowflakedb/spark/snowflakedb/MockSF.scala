/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowflakedb.spark.snowflakedb

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import javax.sql.rowset.RowSetMetaDataImpl

import com.snowflakedb.spark.snowflakedb.Parameters.MergedParameters

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.spark.sql.types.StructType
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Assertions._


/**
 * Helper class for mocking Snowflake / JDBC in unit tests.
 */
class MockSF(
    params: Map[String, String],
    existingTablesAndSchemas: Map[String, StructType],
    jdbcQueriesThatShouldFail: Seq[Regex] = Seq.empty) {

  val mergedParams = MergedParameters(params)

  private[this] val queriesIssued: mutable.Buffer[String] = mutable.Buffer.empty
  def getQueriesIssuedAgainstSF: Seq[String] = queriesIssued.toSeq

  private[this] val jdbcConnections: mutable.Buffer[Connection] = mutable.Buffer.empty

  val jdbcWrapper: JDBCWrapper = spy(new JDBCWrapper)

  private def createMockStatement(query: String) : PreparedStatement = {
    val mockStatement= mock(classOf[PreparedStatement], RETURNS_SMART_NULLS)
    if (jdbcQueriesThatShouldFail.forall(_.findFirstMatchIn(query).isEmpty)) {
      when(mockStatement.execute()).thenReturn(true)

      // Prepare mockResult depending on the query
      var mockResult = mock(classOf[ResultSet], RETURNS_SMART_NULLS)
      if (query.startsWith("COPY INTO '")) {
        // It's a SF->S3 query, SnowflakeRelation checks the schema
        when(mockResult.getMetaData()).thenReturn({
          var md = new RowSetMetaDataImpl
          md.setColumnCount(3)
          md.setColumnName(1, "rows_unloaded")
          md.setColumnTypeName(1, "NUMBER")
          md
        })
        when(mockResult.getInt(anyInt())).thenReturn(4)
        // Return exactly 1 record
        when(mockResult.next()).thenAnswer(new Answer[Boolean] {
          private var count : Int = 0
          override def answer(invocation: InvocationOnMock): Boolean= {
            count += 1
            if (count == 1)
              true
            else
              false
          }
        })
      }
      when(mockStatement.executeQuery()).thenReturn(mockResult)
    } else {
      when(mockStatement.execute()).thenThrow(new SQLException(s"Error executing $query"))
      when(mockStatement.executeQuery()).thenThrow(new SQLException(s"Error executing $query"))
    }
    mockStatement
  }

  private def createMockConnection(): Connection = {
    val conn = mock(classOf[Connection], RETURNS_SMART_NULLS)
    jdbcConnections.append(conn)
    when(conn.prepareStatement(anyString())).thenAnswer(new Answer[PreparedStatement] {
      override def answer(invocation: InvocationOnMock): PreparedStatement = {
        val query = invocation.getArguments()(0).asInstanceOf[String]
        queriesIssued.append(query)
        createMockStatement(query)
      }
    })
    conn
  }

  doAnswer(new Answer[Connection] {
    override def answer(invocation: InvocationOnMock): Connection = createMockConnection()
  }).when(jdbcWrapper).getConnector(any[MergedParameters])

  doAnswer(new Answer[Boolean] {
    override def answer(invocation: InvocationOnMock): Boolean = {
      existingTablesAndSchemas.contains(invocation.getArguments()(1).asInstanceOf[String])
    }
  }).when(jdbcWrapper).tableExists(any[Connection], anyString())

  doAnswer(new Answer[StructType] {
    override def answer(invocation: InvocationOnMock): StructType = {
      existingTablesAndSchemas(invocation.getArguments()(1).asInstanceOf[String])
    }
  }).when(jdbcWrapper).resolveTable(any[Connection], anyString())


  def verifyThatConnectionsWereClosed(): Unit = {
    jdbcConnections.foreach { conn =>
      verify(conn).close()
    }
  }

  def verifyThatExpectedQueriesWereIssued(expectedQueries: Seq[Regex]): Unit = {
    expectedQueries.zip(queriesIssued).foreach { case (expected, actual) =>
      if (expected.findFirstMatchIn(actual).isEmpty) {
        fail(
          s"""
             |Actual and expected JDBC queries did not match:
             |=== Expected:
             |$expected
             |=== Actual:
             |$actual
             |=== END
           """.stripMargin)
      }
    }
    if (expectedQueries.length > queriesIssued.length) {
      val missingQueries = expectedQueries.drop(queriesIssued.length)
      fail(s"Missing ${missingQueries.length} (out of ${expectedQueries.length}) expected JDBC queries:" +
        s"\n${missingQueries.mkString("\n")}")
    } else if (queriesIssued.length > expectedQueries.length) {
      val extraQueries = queriesIssued.drop(expectedQueries.length)
      fail(s"Got ${extraQueries.length} unexpected JDBC queries:\n${extraQueries.mkString("\n")}")
    }
  }

  def verifyThatExpectedQueriesWereIssuedForUnload(expectedQueries: Seq[Regex]): Unit = {
    val queriesWithPrologueAndEpilogue= Seq(Utils.genPrologueSql(mergedParams).r) ++
      expectedQueries
    verifyThatExpectedQueriesWereIssued(queriesWithPrologueAndEpilogue)
  }

}
