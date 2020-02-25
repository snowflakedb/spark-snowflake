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

import org.scalatest.{FunSuite, Matchers}

/**
  * Check validation of parameter config
  */
class ParametersSuite extends FunSuite with Matchers {

  val minParams: Map[String, String] = Map(
    "tempdir" -> "s3://foo/bar",
    "dbtable" -> "test_table",
    "sfurl" -> "account.snowflakecomputing.com:443",
    "sfuser" -> "username",
    "sfpassword" -> "password"
  )

  test("Minimal valid parameter map is accepted") {

    val mergedParams = Parameters.mergeParameters(minParams)

    mergedParams.rootTempDir should startWith(minParams("tempdir"))
    mergedParams.createPerQueryTempDir() should startWith(minParams("tempdir"))
    mergedParams.sfURL shouldBe minParams("sfurl")
    mergedParams.sfUser shouldBe minParams("sfuser")
    mergedParams.sfPassword shouldBe minParams("sfpassword")
    mergedParams.table shouldBe Some(TableName(minParams("dbtable")))

    // Check that the defaults have been added
    Parameters.DEFAULT_PARAMETERS foreach {
      case (key, value) => mergedParams.parameters(key) shouldBe value
    }
  }

  test("createPerQueryTempDir() returns distinct temp paths") {
    val mergedParams = Parameters.mergeParameters(minParams)

    mergedParams.createPerQueryTempDir() should not equal mergedParams
      .createPerQueryTempDir()
  }

  test("Errors are thrown when mandatory parameters are not provided") {
    def checkMerge(params: Map[String, String]): Unit = {
      intercept[IllegalArgumentException] {
        Parameters.mergeParameters(params)
      }
    }

    val minParamsStage = minParams.filterNot(x => x._1 == "tempdir")

    // Check that removing any of the parameters causes a failure
    for ((k, _) <- minParamsStage) {
      val params = collection.mutable.Map() ++= minParamsStage
      params.remove(k)
      checkMerge(params.toMap)
    }
  }

  test("Must specify either 'dbtable' or 'query' parameter, but not both") {
    intercept[IllegalArgumentException] {
      val params = collection.mutable.Map() ++= minParams
      params.remove("dbtable")
      Parameters.mergeParameters(params.toMap)
    }.getMessage should (include("dbtable") and include("query"))

    intercept[IllegalArgumentException] {
      val params = collection.mutable.Map() ++= minParams
      params += "query" -> "select * from test_table"
      Parameters.mergeParameters(params.toMap)
    }.getMessage should (include("dbtable") and include("query") and include(
      "both"
    ))

    val params = collection.mutable.Map() ++= minParams
    params.remove("dbtable")
    params += "query" -> "select * from test_table"
    Parameters.mergeParameters(params.toMap)
  }

  test("See if we can specify role") {
    val params = collection.mutable.Map() ++= minParams
    params += "sfrole" -> "admin"
    val mergedParams = Parameters.mergeParameters(params.toMap)
    mergedParams.sfRole shouldBe Some("admin")
  }

  //  add Authenticator and OAuth parameter tests

  test("See if we can specify Authenticator/token") {
    val params = collection.mutable.Map() ++= minParams
    params += "sfauthenticator" -> "oauth"
    params += "sftoken" -> "mytoken"
    params.remove("sfpassword")
    val mergedParams = Parameters.mergeParameters(params.toMap)
    mergedParams.sfAuthenticator shouldBe Some("oauth")
    mergedParams.sfToken shouldBe Some("mytoken")
  }

  test("Must specify OAuth Token when Authenticator mode equals 'oauth'") {
    intercept[IllegalArgumentException] {
      val params = collection.mutable.Map() ++= minParams
      params += "sfauthenticator" -> "oauth"
      params.remove("sftoken")
      Parameters.mergeParameters(params.toMap)
    }.getMessage should (include("token") and include("required") and
      include("authenticator")  )
  }

  test("Authenticator mode must be 'oauth' when an OAuth token is specified") {
    intercept[IllegalArgumentException] {
      val params = collection.mutable.Map() ++= minParams
      params += "sftoken" -> "mytoken"
      Parameters.mergeParameters(params.toMap)
    }.getMessage should (include("token") and include("Invalid") and
      include("authenticator")  )
  }

}
