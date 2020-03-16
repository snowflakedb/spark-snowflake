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

import java.net.URI

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
      removeCreds("s3n://ACCESSKEY:SECRETKEY@bucket/path/to/temp/dir") ===
        "s3n://bucket/path/to/temp/dir"
    )
  }

  test("test Utils.getPrettySizeString") {
    assert(Utils.getSizeString(100) === "100 Bytes")
    assert(Utils.getSizeString(1024) === "1.00 KB")
    assert(Utils.getSizeString((1.1 * 1024).toLong) === "1.10 KB")
    assert(Utils.getSizeString((1.25 * 1024 * 1024).toLong) === "1.25 MB")
    assert(Utils.getSizeString((1.88 * 1024 * 1024 * 1024 + 1).toLong) === "1.88 GB")
    assert(Utils.getSizeString((3.51 * 1024 * 1024 * 1024 * 1024 + 100).toLong) === "3.51 TB")
  }
}
