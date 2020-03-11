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
  val mergedParams = MergedParameters(Parameters.DEFAULT_PARAMETERS)
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

}
