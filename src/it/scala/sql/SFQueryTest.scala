package org.apache.spark.sql.thundersnow

import java.sql.Date
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

  // disable CheckToRDD
  override def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF =
      try df
      catch {
        case ae: AnalysisException =>
          if (ae.plan.isDefined) {
            fail(s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
          } else {
            throw ae
          }
      }

    // disable check RDD
    SFQueryTest.checkAnswer(analyzedDF, expectedAnswer, checkToRDD = false)
  }
}

object SFQueryTest {

  def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // Note this returns 0.0 and -0.0 as same
    case (a: Double, b: Double) => (a - b).abs < 0.0001
    case (a: Float, b: Float) => (a - b).abs < 0.0001
    case (a: Double, b: java.math.BigDecimal) => (a - b.toString.toDouble).abs < 0.0001
    case (a: Float, b: java.math.BigDecimal) => (a - b.toString.toFloat).abs < 0.0001
    case (a: java.math.BigDecimal, b: Double) => (a.toString.toDouble - b).abs < 0.0001
    case (a: java.math.BigDecimal, b: Float) => (a.toString.toFloat - b).abs < 0.0001
    case (a: java.math.BigDecimal, b: java.math.BigDecimal) => a.equals(b)
    case (a: Date, b: Date) => a.toString == b.toString
    case (a, b) => a == b
  }

  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row], checkToRDD: Boolean = true): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer, checkToRDD) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  def getErrorMessageInCheckAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    if (checkToRDD) {
      SQLExecution.withSQLConfPropagated(df.sparkSession) {
        df.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
      }
    }

    val sparkAnswer = try df.collect().toSeq
    catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |${df.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (!compare(prepareAnswer(expectedAnswer, isSorted), prepareAnswer(sparkAnswer, isSorted))) {
      return Some(genError(expectedAnswer, sparkAnswer, isSorted))
    }
    None
  }

  private def genError(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): String = {
    val getRowType: Option[Row] => String = row =>
      row
        .map(row =>
          if (row.schema == null) {
            "struct<>"
          } else {
            s"${row.schema.catalogString}"
        })
        .getOrElse("struct<>")

    s"""
       |== Results ==
       |${sideBySide(
         s"== Correct Answer - ${expectedAnswer.size} ==" +:
           getRowType(expectedAnswer.headOption) +:
           prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
         s"== Spark Answer - ${sparkAnswer.size} ==" +:
           getRowType(sparkAnswer.headOption) +:
           prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")}
    """.stripMargin
  }
}
