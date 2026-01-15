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

  //  Add Authenticator and OAuth parameter tests

  test("See if we can specify Authenticator/token") {
    val params = collection.mutable.Map() ++= minParams
    params += "sfauthenticator" -> "oauth"
    params += "sftoken" -> "mytoken"
    params.remove("sfpassword")
    params.remove("sfuser")
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
      params.remove("sfpassword")
      Parameters.mergeParameters(params.toMap)
    }.getMessage should (include("token") and include("authenticator") and
      include("must be set to 'oauth'")  )
  }

  test("See if we can specify Authenticator with Workload Identity Provider") {
    val params = collection.mutable.Map() ++= minParams
    params += "sfauthenticator" -> "WORKLOAD_IDENTITY"
    params += "sfworkloadidentityprovider" -> "my_provider"
    params.remove("sfpassword")
    params.remove("sfuser")
    val mergedParams = Parameters.mergeParameters(params.toMap)
    mergedParams.sfAuthenticator shouldBe Some("WORKLOAD_IDENTITY")
    mergedParams.sfWorkloadIdentityProvider shouldBe Some("my_provider")
  }

  test("Must specify Workload Identity Provider when Authenticator mode equals 'WORKLOAD_IDENTITY'") {
    intercept[IllegalArgumentException] {
      val params = collection.mutable.Map() ++= minParams
      params += "sfauthenticator" -> "WORKLOAD_IDENTITY"
      params.remove("sfworkloadidentityprovider")
      params.remove("sfpassword")
      Parameters.mergeParameters(params.toMap)
    }.getMessage should (include("workload identity provider") and include("is required") and
      include("WORKLOAD_IDENTITY"))
  }

  test("Workload Identity Provider validation is case-insensitive for authenticator") {
    intercept[IllegalArgumentException] {
      val params = collection.mutable.Map() ++= minParams
      params += "sfauthenticator" -> "workload_identity"
      params.remove("sfworkloadidentityprovider")
      params.remove("sfpassword")
      Parameters.mergeParameters(params.toMap)
    }.getMessage should (include("workload identity provider") and include("is required"))
  }

  test("test ConnectionCacheKey.isQueryInWhiteList()") {
    val mergedParams = Parameters.mergeParameters(minParams)
    val connectionCacheKey = new ConnectionCacheKey(mergedParams)

    // test create table
    assert(connectionCacheKey.isQueryInWhiteList("create table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList("create \tor replace table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList("create \tor replace temp table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList(
      "create \tor replace temporary table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList(
      "create \tor replace\tglobal temp table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList(
      "create \tor replace\tglobal temporary table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList(
      "create \tor replace local temp table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList(
      "create \tor replace local temporary table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList(
      " create \tor replace transient table t1 (c1 int)"))
    assert(connectionCacheKey.isQueryInWhiteList(
      " create \tor replace volatile table t1 (c1 int)"))

    // test create stage
    assert(connectionCacheKey.isQueryInWhiteList("create stage st1;"))
    assert(connectionCacheKey.isQueryInWhiteList("create temp stage\t st1"))
    assert(connectionCacheKey.isQueryInWhiteList("create temporary stage st1"))
    assert(connectionCacheKey.isQueryInWhiteList("create or replace stage st1"))
    assert(connectionCacheKey.isQueryInWhiteList("create or replace\ttemp stage st1"))
    assert(connectionCacheKey.isQueryInWhiteList("create or replace temporary stage st1"))

    // test drop table/stage
    assert(connectionCacheKey.isQueryInWhiteList("drop stage st1;"))
    assert(connectionCacheKey.isQueryInWhiteList("drop\t stage if exists st1;"))
    assert(connectionCacheKey.isQueryInWhiteList("drop table t1;"))
    assert(connectionCacheKey.isQueryInWhiteList("drop\t table if exists t1;"))

    // merge into
    assert(connectionCacheKey.isQueryInWhiteList("merge into t1 using t2 ..."))
    assert(connectionCacheKey.isQueryInWhiteList(" merge into t1 using t2 ..."))
    assert(connectionCacheKey.isQueryInWhiteList("\tmerge\tinto t1 using t2 ..."))

    // negative test
    assert(!connectionCacheKey.isQueryInWhiteList("create database db1"))
    assert(!connectionCacheKey.isQueryInWhiteList("create schema sc1"))
    assert(!connectionCacheKey.isQueryInWhiteList("DROP something"))
    assert(!connectionCacheKey.isQueryInWhiteList("use role role_1"))
    assert(!connectionCacheKey.isQueryInWhiteList("use warehouse role_1"))
  }

  test("test ConnectionCacheKey.isConnectionCacheSupported()") {
    val enableShareMap: Map[String, String] =
      Map((Parameters.PARAM_SUPPORT_SHARE_CONNECTION -> "on"))

    // default preactions and postactions
    var mergedParams = Parameters.mergeParameters(minParams ++ enableShareMap)
    var connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(connectionCacheKey.isConnectionCacheSupported)

    val whiteListQuery1 = "create table t1(c1 int)"
    // preactions has 1 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_PREACTIONS -> whiteListQuery1))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(connectionCacheKey.isConnectionCacheSupported)

    // postactions has 1 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_POSTACTIONS -> whiteListQuery1))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(connectionCacheKey.isConnectionCacheSupported)

    val whiteListQuery2 = "create stage st1;drop table t1"
    // preactions has 2 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_PREACTIONS -> whiteListQuery2))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(connectionCacheKey.isConnectionCacheSupported)

    // postactions has 2 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_POSTACTIONS -> whiteListQuery2))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(connectionCacheKey.isConnectionCacheSupported)
  }

  test("negative test ConnectionCacheKey.isConnectionCacheSupported(): " +
    "support_share_connection = false") {
    val enableShareMap: Map[String, String] =
      Map((Parameters.PARAM_SUPPORT_SHARE_CONNECTION -> "off"))

    // default preactions and postactions
    var mergedParams = Parameters.mergeParameters(minParams ++ enableShareMap)
    var connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)

    val whiteListQuery1 = "create table t1(c1 int)"
    // preactions has 1 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ enableShareMap ++ Map(Parameters.PARAM_PREACTIONS -> whiteListQuery1))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)

    // postactions has 1 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ enableShareMap ++ Map(Parameters.PARAM_POSTACTIONS -> whiteListQuery1))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)

    val whiteListQuery2 = "create stage st1;drop table t1"
    // preactions has 2 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ enableShareMap ++ Map(Parameters.PARAM_PREACTIONS -> whiteListQuery2))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)

    // postactions has 2 white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ enableShareMap ++ Map(Parameters.PARAM_POSTACTIONS -> whiteListQuery2))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)
  }

  test("negative test ConnectionCacheKey.isConnectionCacheSupported(): " +
    "query not in white list") {
    val listQuery1 = "create database db1"
    // preactions has 1 non-white list query
    var mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_PREACTIONS -> listQuery1))
    var connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)

    // postactions has 1 non-white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_POSTACTIONS -> listQuery1))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)

    val listQuery2 = "create stage st1; create database db1"
    // preactions has 2 non-white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_PREACTIONS -> listQuery2))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)

    // postactions has 2 non-white list query
    mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_POSTACTIONS -> listQuery2))
    connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isConnectionCacheSupported)
  }

  test("test ConnectionCacheKey.isPrePostActionsQualifiedForConnectionShare()") {
    val listQuery1 = "create database db1"
    val mergedParams = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_POSTACTIONS -> listQuery1))
    assert(!mergedParams.forceSkipPrePostActionsCheck)
    val connectionCacheKey = new ConnectionCacheKey(mergedParams)
    assert(!connectionCacheKey.isPrePostActionsQualifiedForConnectionShare)

    val mergedParamsEnabled = Parameters.mergeParameters(minParams ++
      Map(Parameters.PARAM_FORCE_SKIP_PRE_POST_ACTION_CHECK_FOR_SESSION_SHARING -> "true",
        Parameters.PARAM_POSTACTIONS -> listQuery1))
    assert(mergedParamsEnabled.forceSkipPrePostActionsCheck)
    val forceEnabledConnectionCacheKey = new ConnectionCacheKey(mergedParamsEnabled)
    assert(forceEnabledConnectionCacheKey.isPrePostActionsQualifiedForConnectionShare)
  }

  test("test snowflake_stage parameter") {
    // Test that snowflakeStage is None when not specified
    val mergedParams = Parameters.mergeParameters(minParams)
    mergedParams.snowflakeStage shouldBe None

    // Test that snowflakeStage is correctly set when specified
    val paramsWithStage = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_SNOWFLAKE_STAGE -> "my_custom_stage"))
    paramsWithStage.snowflakeStage shouldBe Some("my_custom_stage")

    // Test with different stage name formats
    val paramsWithQuotedStage = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_SNOWFLAKE_STAGE -> "\"my_quoted_stage\""))
    paramsWithQuotedStage.snowflakeStage shouldBe Some("\"my_quoted_stage\"")

    // Test with fully qualified stage name
    val paramsWithFullyQualifiedStage = Parameters.mergeParameters(
      minParams ++ Map(Parameters.PARAM_SNOWFLAKE_STAGE -> "mydb.myschema.my_stage"))
    paramsWithFullyQualifiedStage.snowflakeStage shouldBe Some("mydb.myschema.my_stage")
  }
}
