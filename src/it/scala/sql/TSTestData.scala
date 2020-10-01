package org.apache.spark.sql.thundersnow

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SQLTestData
import org.apache.spark.sql.test.SQLTestData._

/**
 * Most test data in SQLTestData invokes RDD API, which is not supported by TS yet.
 * Please override all test data used by TS test suites in this class
 */
trait TSTestData extends SQLTestData {

  import org.apache.spark.sql.test.SQLTestData.CourseSales

  override protected lazy val testData: DataFrame =
    spark.createDataFrame((1 to 100).map(i => TestData(i, i.toString)))

  override protected lazy val testData2: DataFrame =
    spark.createDataFrame(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 1) ::
        TestData2(2, 2) ::
        TestData2(3, 1) ::
        TestData2(3, 2) :: Nil)

  override protected lazy val testData3: DataFrame =
    spark.createDataFrame(
      TestData3(1, None) ::
        TestData3(2, Some(2)) :: Nil)

  override protected lazy val decimalData: DataFrame =
    spark.createDataFrame(
      DecimalData(1, 1) ::
        DecimalData(1, 2) ::
        DecimalData(2, 1) ::
        DecimalData(2, 2) ::
        DecimalData(3, 1) ::
        DecimalData(3, 2) :: Nil)

  override protected lazy val courseSales: DataFrame =
    spark
      .createDataFrame(
        CourseSales("dotNET", 2012, 10000) ::
          CourseSales("Java", 2012, 20000) ::
          CourseSales("dotNET", 2012, 5000) ::
          CourseSales("dotNET", 2013, 48000) ::
          CourseSales("Java", 2013, 30000) :: Nil)

  override protected lazy val lowerCaseData: DataFrame =
    spark
      .createDataFrame(
        LowerCaseData(1, "a") ::
          LowerCaseData(2, "b") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(4, "d") :: Nil)

  override protected lazy val upperCaseData: DataFrame =
    spark
      .createDataFrame(
        UpperCaseData(1, "A") ::
          UpperCaseData(2, "B") ::
          UpperCaseData(3, "C") ::
          UpperCaseData(4, "D") ::
          UpperCaseData(5, "E") ::
          UpperCaseData(6, "F") :: Nil)

  override protected lazy val nullInts: DataFrame =
    spark
      .createDataFrame(
        NullInts(1) ::
          NullInts(2) ::
          NullInts(3) ::
          NullInts(null) :: Nil)

  override protected lazy val allNulls: DataFrame =
    spark
      .createDataFrame(
        NullInts(null) ::
          NullInts(null) ::
          NullInts(null) ::
          NullInts(null) :: Nil)

  override protected lazy val nullStrings: DataFrame =
    spark
      .createDataFrame(
        NullStrings(1, "abc") ::
          NullStrings(2, "ABC") ::
          NullStrings(3, null) :: Nil)

  override protected lazy val lowerCaseDataWithDuplicates: DataFrame =
    spark
      .createDataFrame(
        LowerCaseData(1, "a") ::
          LowerCaseData(2, "b") ::
          LowerCaseData(2, "b") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(3, "c") ::
          LowerCaseData(4, "d") :: Nil)

}
