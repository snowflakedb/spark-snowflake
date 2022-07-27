/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 TouchType Ltd
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

package net.snowflake.spark.snowflake

import java.io.File
import java.net.URI

import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

/**
  * Unit tests for helper functions
  */
class UtilsSuite extends FunSuite with Matchers {

  test("joinUrls preserves protocol information") {
    Utils.joinUrls("s3n://foo/bar/", "/baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "/baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz/") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar/", "baz") shouldBe "s3n://foo/bar/baz/"
    Utils.joinUrls("s3n://foo/bar", "baz") shouldBe "s3n://foo/bar/baz/"
  }

  test("fixUrl produces Snowflake-compatible equivalents") {
    Utils.fixS3Url("s3a://foo/bar/12345") shouldBe "s3://foo/bar/12345"
    Utils.fixS3Url("s3n://foo/bar/baz") shouldBe "s3://foo/bar/baz"
  }

  test("fixUrlForCopyCommand produces Snowflake-compatible equivalents") {
    Utils.fixUrlForCopyCommand("s3a://foo/bar/12345") shouldBe "s3://foo/bar/12345"
    Utils.fixUrlForCopyCommand("s3n://foo/bar/baz") shouldBe "s3://foo/bar/baz"
    Utils.fixUrlForCopyCommand("wasb://container@test.azure.com/path") shouldBe
      "azure://test.azure.com/container/path"
    Utils.fixUrlForCopyCommand("wasbs://container@test.azure.com/path") shouldBe
      "azure://test.azure.com/container/path"
  }

  test("temp paths are random subdirectories of root") {
    val root = "s3n://temp/"
    val firstTempPath = Utils.makeTempPath(root)

    Utils.makeTempPath(root) should (startWith(root) and endWith("/")
      and not equal root and not equal firstTempPath)
  }

  test("removeCredentialsFromURI removes AWS access keys") {
    def removeCreds(uri: String): String = {
      Utils.removeCredentialsFromURI(URI.create(uri)).toString
    }
    assert(
      removeCreds("s3n://bucket/path/to/temp/dir") === "s3n://bucket/path/to/temp/dir"
    )
    assert(
      removeCreds(
        "s3n://ACCESSKEY:SECRETKEY@bucket/path/to/temp/dir") === // pragma: allowlist secret
        "s3n://bucket/path/to/temp/dir"
    )
  }

  test("test Utils.getSizeString") {
    assert(Utils.getSizeString(100) === "100 Bytes")
    assert(Utils.getSizeString(1024) === "1.00 KB")
    assert(Utils.getSizeString((1.1 * 1024).toLong) === "1.10 KB")
    assert(Utils.getSizeString((1.25 * 1024 * 1024).toLong) === "1.25 MB")
    assert(Utils.getSizeString((1.88 * 1024 * 1024 * 1024 + 1).toLong) === "1.88 GB")
    assert(Utils.getSizeString((3.51 * 1024 * 1024 * 1024 * 1024 + 100).toLong) === "3.51 TB")
  }

  test("test Utils.getTimeString") {
    assert(Utils.getTimeString(100) === "100 ms")
    assert(Utils.getTimeString(1000) === "1.00 seconds")
    assert(Utils.getTimeString((1.1 * 1000).toLong) === "1.10 seconds")
    assert(Utils.getTimeString((1.25 * 1000 * 60).toLong) === "1.25 minutes")
    assert(Utils.getTimeString((1.88 * 1000 * 60 * 60 + 1).toLong) === "1.88 hours")
    assert(Utils.getTimeString((188 * 1000 * 60 * 60 + 1).toLong) === "188.00 hours")
  }

  private def writeTempFile(content: String): (File, String, String) = {
    val temp_file = File.createTempFile("test_file_", ".csv")
    val temp_file_full_name = temp_file.getPath
    val temp_file_name = temp_file.getName
    FileUtils.write(temp_file, content)
    (temp_file, temp_file_full_name, temp_file_name)
  }

  test("test Utils.readMapFromFile/readMapFromString") {
    // Test valid map file
    val mapContentString = "#key0 = value0\nkey1=value1\nkey2=value2"
    val (file, fullName, name) = writeTempFile(mapContentString)
    try {
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName("SnowflakeSourceSuite")
      val sc = SparkContext.getOrCreate(conf)
      val resultMap = Utils.readMapFromFile(sc, fullName)
      assert(resultMap.size == 2)
      assert(resultMap("key1").equals("value1"))
      assert(resultMap("key2").equals("value2"))
    } finally {
      FileUtils.deleteQuietly(file)
    }

    // negative invalid mapstring.
    assertThrows[Exception]({
      Utils.readMapFromString("invalid_map_string")
    })
  }

  test("misc in Utils") {
    assert(Utils.getLastCopyUnload == null)
    assert(Utils.getLastPutCommand == null)
    assert(Utils.getLastGetCommand == null)
  }

  test("Utils.getTableNameForExistenceCheck") {
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "t1")
      .equals(""""DB"."SCHEMA".t1"""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "s1.t1")
      .equals(""""DB".s1.t1"""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "d1.s1.t1")
      .equals("""d1.s1.t1"""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "d1..t1")
      .equals("""d1..t1"""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\"s1\".t1")
      .equals(""""DB"."s1".t1"""))

    // Db and schema has special character
    assert(Utils.getTableNameForExistenceCheck("db.1", "schema!1", "\"t1\"")
      .equals(""""db.1"."schema!1"."t1""""))
    assert(Utils.getTableNameForExistenceCheck("db.1", "schema!1", "\"s1\".\"t1\"")
      .equals(""""db.1"."s1"."t1""""))
    assert(Utils.getTableNameForExistenceCheck("db.1", "schema!1", "\"d1\".\"s1\".\"t1\"")
      .equals(""""d1"."s1"."t1""""))
    assert(Utils.getTableNameForExistenceCheck("db.1", "schema!1", "\"d1\"..\"t1\"")
      .equals(""""d1".."t1""""))

    // Table name or schema name have quoted dot.
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\"table1.\"")
      .equals(""""DB"."SCHEMA"."table1.""""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\"table.1\"")
      .equals(""""DB"."SCHEMA"."table.1""""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\".table1\"")
      .equals(""""DB"."SCHEMA".".table1""""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\"sc1.\".\"table1.\"")
      .equals(""""DB"."sc1."."table1.""""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\"sc.1\".\"table1.\"")
      .equals(""""DB"."sc.1"."table1.""""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\".sc1\".\"table1.\"")
      .equals(""""DB".".sc1"."table1.""""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\"d1.\".\"sc1.\".\"table1.\"")
      .equals("\"d1.\".\"sc1.\".\"table1.\""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\"d.1\".\"sc.1\".\"table1.\"")
      .equals("\"d.1\".\"sc.1\".\"table1.\""))
    assert(Utils.getTableNameForExistenceCheck("db", "schema", "\".d1\".\".sc1\".\"table1.\"")
      .equals("\".d1\".\".sc1\".\"table1.\""))

    // If the name is invalid, just get the original name.
    val invalidNames =
      Seq(".t1", "t1.", ".", "..", "...", ".s1.t1", ".d1.s1.t1", "d1.s1.t1.", "d1...t1")
    invalidNames.foreach { s =>
      assert(Utils.getTableNameForExistenceCheck("sfDb", "sfSchema", s).equals(s))
    }
  }

}
