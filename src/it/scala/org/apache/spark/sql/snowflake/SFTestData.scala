package org.apache.spark.sql.snowflake

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SQLTestData
import org.apache.spark.sql.test.SQLTestData._

/**
 * Most test data in SQLTestData invokes RDD API, which is not supported by TS yet.
 * Please override all test data used by TS test suites in this class
 */
trait SFTestData extends SQLTestData {

  import org.apache.spark.sql.test.SQLTestData.CourseSales

  override protected lazy val testData: DataFrame = {
    val df = spark.createDataFrame((1 to 100).map(i => TestData(i, i.toString)))
    df.createOrReplaceTempView("testData")
    df
  }

  override protected lazy val testData2: DataFrame = {
    val df = spark.createDataFrame(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 1) ::
        TestData2(2, 2) ::
        TestData2(3, 1) ::
        TestData2(3, 2) :: Nil)
      df.createOrReplaceTempView("testData2")
      df
}

  override protected lazy val testData3: DataFrame = {
    val df = spark.createDataFrame(
      TestData3(1, None) ::
        TestData3(2, Some(2)) :: Nil)
    df.createOrReplaceTempView("testData3")
    df
  }

  override protected lazy val decimalData: DataFrame = {
    val df = spark.createDataFrame(
      DecimalData(1, 1) ::
        DecimalData(1, 2) ::
        DecimalData(2, 1) ::
        DecimalData(2, 2) ::
        DecimalData(3, 1) ::
        DecimalData(3, 2) :: Nil)
    df.createOrReplaceTempView("decimalData")
    df
  }

  override protected lazy val courseSales: DataFrame = {
    val df = spark
      .createDataFrame(
        CourseSales("dotNET", 2012, 10000) ::
          CourseSales("Java", 2012, 20000) ::
          CourseSales("dotNET", 2012, 5000) ::
          CourseSales("dotNET", 2013, 48000) ::
          CourseSales("Java", 2013, 30000) :: Nil)
    df.createOrReplaceTempView("courseSales")
    df
  }

  override protected lazy val lowerCaseData: DataFrame = {
    val df = spark
      .createDataFrame(
        LowerCaseData(1, "a") ::
          LowerCaseData(2, "b") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(4, "d") :: Nil)
    df.createOrReplaceTempView("lowerCaseData")
    df
  }

  override protected lazy val upperCaseData: DataFrame = {
    val df = spark
      .createDataFrame(
        UpperCaseData(1, "A") ::
          UpperCaseData(2, "B") ::
          UpperCaseData(3, "C") ::
          UpperCaseData(4, "D") ::
          UpperCaseData(5, "E") ::
          UpperCaseData(6, "F") :: Nil)
    df.createOrReplaceTempView("upperCaseData")
    df
  }

  override protected lazy val nullInts: DataFrame = {
    val df = spark
      .createDataFrame(
        NullInts(1) ::
          NullInts(2) ::
          NullInts(3) ::
          NullInts(null) :: Nil)
    df.createOrReplaceTempView("nullInts")
    df
  }

  override protected lazy val allNulls: DataFrame = {
    val df = spark
      .createDataFrame(
        NullInts(null) ::
          NullInts(null) ::
          NullInts(null) ::
          NullInts(null) :: Nil)
    df.createOrReplaceTempView("allNulls")
    df
  }

  override protected lazy val nullStrings: DataFrame = {
    val df = spark
      .createDataFrame(
        NullStrings(1, "abc") ::
          NullStrings(2, "ABC") ::
          NullStrings(3, null) :: Nil)
    df.createOrReplaceTempView("nullStrings")
    df
  }

  override protected lazy val lowerCaseDataWithDuplicates: DataFrame = {
    val df = spark
      .createDataFrame(
        LowerCaseData(1, "a") ::
          LowerCaseData(2, "b") ::
          LowerCaseData(2, "b") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(4, "d") :: Nil)
    df.createOrReplaceTempView("lowerCaseDataWithDuplicates")
    df
  }

}
