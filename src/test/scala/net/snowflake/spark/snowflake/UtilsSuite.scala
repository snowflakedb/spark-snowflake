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
      removeCreds("s3n://ACCESSKEY:SECRETKEY@bucket/path/to/temp/dir") === // pragma: allowlist secret
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
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SnowflakeSourceSuite")
    val sc = SparkContext.getOrCreate(conf)

    // Test valid map file
    val mapContentString = "#key0 = value0\nkey1=value1\nkey2=value2"
    val (file, fullName, name) = writeTempFile(mapContentString)
    try {
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

  test("Utils.getFullyQualifiedName()") {
    case class TestItem(sfDatabase: String,
                        sfSchema: String,
                        tableName: String,
                        fullyQualifiedName: String)
    val TestItems = Seq(
      // un-quoted database & schema, un-quoted table name
      TestItem("test_db", "test_schema", "table1", s""""TEST_DB"."TEST_SCHEMA".table1"""),
      TestItem("test_db", "test_schema", "schema1.table1", s""""TEST_DB".schema1.table1"""),
      TestItem("test_db", "test_schema", "database1.schema1.table1", s"""database1.schema1.table1"""),
      TestItem("test_db", "test_schema", "database1..table1", s"""database1."TEST_SCHEMA".table1"""),
      // quoted database & schema, un-quoted table name
      TestItem("test_db", "test_schema", "\"schema1 !\".table1", s""""TEST_DB"."schema1 !".table1"""),
      TestItem("test_db", "test_schema", "\"database1 .\".\"schema1 !\".table1", s""""database1 ."."schema1 !".table1"""),
      TestItem("test_db", "test_schema", "\"database1 .\"..table1", s""""database1 ."."TEST_SCHEMA".table1"""),
      // un-quoted database & schema, quoted table name
      TestItem("test_db", "test_schema", "\"table1\"", s""""TEST_DB"."TEST_SCHEMA"."table1""""),
      TestItem("test_db", "test_schema", "schema1.\"table1\"", s""""TEST_DB".schema1."table1""""),
      TestItem("test_db", "test_schema", "database1.schema1.\"table1\"", s"""database1.schema1."table1""""),
      TestItem("test_db", "test_schema", "database1..\"table1\"", s"""database1."TEST_SCHEMA"."table1""""),
      // quoted database & schema, quoted table name
      TestItem("test_db", "test_schema", "\"schema1 !\".\"table1\"", s""""TEST_DB"."schema1 !"."table1""""),
      TestItem("test_db", "test_schema", "\"database1 .\".\"schema1 !\".\"table1\"", s""""database1 ."."schema1 !"."table1""""),
      TestItem("test_db", "test_schema", "\"database1 .\"..\"table1\"", s""""database1 ."."TEST_SCHEMA"."table1""""),
      // sfDatabase and sfSchema are quoted.
      TestItem("\"test_db\"", "\"test_schema\"", "table1", s""""test_db"."test_schema".table1"""),
      TestItem("test db", "test ! schema", "table1", s""""test db"."test ! schema".table1"""),
    )

    TestItems.foreach(item => {
      val sfOptions = Map(
        Parameters.PARAM_SF_DATABASE -> item.sfDatabase,
        Parameters.PARAM_SF_SCHEMA -> item.sfSchema
      )
      val param = Parameters.MergedParameters(sfOptions)
      // println(s"${item.sfDatabase}, ${item.sfSchema}, ${item.tableName} -> ${item.fullyQualifiedName}")
      // println(Utils.getFullyQualifiedName(item.tableName, param))
      assert(Utils.getFullyQualifiedName(item.tableName, param)
        .equals(item.fullyQualifiedName))
    })
  }

  test("Utils.splitNameAs2Parts") {
    var v1 = (Option(""), "")
    v1 = Utils.splitNameAs2Parts("name1")
    assert(v1._1.isEmpty && v1._2.equals("name1"))
    v1 = Utils.splitNameAs2Parts("schema1.name1")
    assert(v1._1.get.equals("schema1") && v1._2.equals("name1"))
    v1 = Utils.splitNameAs2Parts("\"schema1\".name1")
    assert(v1._1.get.equals("\"schema1\"") && v1._2.equals("name1"))
    v1 = Utils.splitNameAs2Parts("database1.schema1.name1")
    assert(v1._1.get.equals("database1.schema1") && v1._2.equals("name1"))

    v1 = Utils.splitNameAs2Parts("\"name1\"")
    assert(v1._1.isEmpty && v1._2.equals("\"name1\""))
    v1 = Utils.splitNameAs2Parts("schema1.\"name1\"")
    assert(v1._1.get.equals("schema1") && v1._2.equals("\"name1\""))
    v1 = Utils.splitNameAs2Parts("\"schema1\".\"name1\"")
    assert(v1._1.get.equals("\"schema1\"") && v1._2.equals("\"name1\""))
    v1 = Utils.splitNameAs2Parts("\"database1\".\"schema1\".\"name1\"")
    assert(v1._1.get.equals("\"database1\".\"schema1\"") && v1._2.equals("\"name1\""))

    // negative test
    assertThrows[Exception](Utils.splitNameAs2Parts("name1\""))
  }

}
