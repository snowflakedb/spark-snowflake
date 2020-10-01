package org.apache.spark.sql.thundersnow

import java.util.TimeZone

import org.apache.spark.sql.QueryTest.{fail, prepareAnswer}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.util.{sideBySide, stackTraceToString}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.scalactic.source.Position
import org.scalatest.Tag

trait SFQueryTest extends QueryTest {

  // override blackList to block unsupported test functions
  protected def blackList: Seq[String]

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit
      pos: Position): Unit = {
    if (blackList.contains(testName)) {
      logInfo(s"---> Skip $testName")
    } else super.test(testName, testTags: _*)(testFun)
  }
}
