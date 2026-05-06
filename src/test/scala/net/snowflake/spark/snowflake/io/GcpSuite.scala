/*
 * Copyright 2015-2020 Snowflake Computing
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
package net.snowflake.spark.snowflake.io

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.{
  Parameters,
  SnowflakeConnectorFeatureNotSupportException
}
import org.scalatest.FunSuite

class GcpSuite extends FunSuite {
  var mergedParams = MergedParameters(Parameters.DEFAULT_PARAMETERS
                      ++ Map("sfurl" -> "sfctest0.snowflakecomputing.com"))
  val gcsStorage = InternalGcsStorage(mergedParams, "dummyStage", null, null)

  test("These functions are not supported for GCP") {
    assertThrows[SnowflakeConnectorFeatureNotSupportException] {
      gcsStorage.download(null)
    }
    assertThrows[SnowflakeConnectorFeatureNotSupportException] {
      gcsStorage.deleteFile(null)
    }
    assertThrows[SnowflakeConnectorFeatureNotSupportException] {
      gcsStorage.deleteFiles(null)
    }
    assertThrows[SnowflakeConnectorFeatureNotSupportException] {
      gcsStorage.fileExists(null)
    }
  }

  // By default, IT uses S3UploadOutputStream.write(b, off, len) only.
  // This test is used to test S3UploadOutputStream.write(int)
  test(" S3UploadOutputStream.write") {
    val storageInfo = Map(
      StorageInfo.BUCKET_NAME -> "test_bucket",
      StorageInfo.PREFIX -> "test_prefix"
    )
    val s3UploadStream = new S3UploadOutputStream(
      null, null, storageInfo, 128, "file_name")

    // write some bytes
    for (i <- 1 to 128) {
      s3UploadStream.write(i)
    }
    val dataInBuffer = s3UploadStream.getDataInBufferForTest()
    // Verify value to be correct
    for (i <- 1 to 128) {
      assert(dataInBuffer(i - 1) == i.toByte)
    }
    // write one more byte will cause exception because 's3Client' is not set
    assertThrows[Exception]({
      s3UploadStream.write(100)
    })
  }

}
