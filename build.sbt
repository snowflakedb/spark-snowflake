/*
 * Copyright 2015-2019 Snowflake Computing
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

import sbt._
import Keys._
import sbtrelease.ReleasePlugin.autoImport._

import scala.util.Properties

val sparkVersion = "2.2.0"
val testSparkVersion = sys.props.get("spark.testVersion").getOrElse(sparkVersion)

lazy val ItTest = config("it") extend(Test)

lazy val root = Project("spark-snowflake", file("."))
  .configs(ItTest)
  .settings(inConfig(ItTest)(Defaults.testSettings) : _*)
  .settings(Defaults.coreDefaultSettings: _*)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "spark-snowflake",
    organization := "net.snowflake",
    version := "2.4.12-spark_2.2",
    scalaVersion := sys.props.getOrElse("SPARK_SCALA_VERSION", default = "2.11.12"),
    crossScalaVersions := Seq("2.10.5","2.11.12"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    resolvers +=
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    libraryDependencies ++= Seq(
      "net.snowflake" % "snowflake-ingest-sdk" % "0.9.5",
      "net.snowflake" % "snowflake-jdbc" % "3.6.24",
      "com.google.guava" % "guava" % "14.0.1" % Test,
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.mockito" % "mockito-core" % "1.10.19" % Test,
      "org.apache.commons" % "commons-lang3" % "3.5",

      "org.apache.spark" %% "spark-core" % testSparkVersion % "provided, test",
      "org.apache.spark" %% "spark-sql" % testSparkVersion % "provided, test",
      "org.apache.spark" %% "spark-hive" % testSparkVersion % "provided, test"
    ),

    testOptions in Test += Tests.Argument("-oF"),
    fork in Test := true,
    javaOptions in Test ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M"),

    //Release settings
    com.typesafe.sbt.SbtPgp.autoImportImpl.usePgpKeyHex(Properties.envOrElse("GPG_SIGNATURE", "12345")),
    com.typesafe.sbt.pgp.PgpKeys.pgpPassphrase in Global := Properties.envOrNone("GPG_KEY_PASSPHRASE").map(pw => pw.toCharArray),

    publishMavenStyle := true,
    releaseCrossBuild := true,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,

    pomExtra :=
      <url>https://github.com/snowflakedb/spark-snowflake</url>
        <scm>
          <url>git@github.com:snowflakedb/spark-snowflake.git</url>
          <connection>scm:git:git@github.com:snowflakedb/spark-snowflake.git</connection>
        </scm>
        <developers>
          <developer>
            <id>MarcinZukowski</id>
            <name>Marcin Zukowski</name>
            <url>https://github.com/MarcinZukowski</url>
          </developer>
          <developer>
            <id>etduwx</id>
            <name>Edward Ma</name>
            <url>https://github.com/etduwx</url>
          </developer>
          <developer>
            <id>binglihub</id>
            <name>Bing Li</name>
            <url>https://github.com/binglihub</url>
          </developer>
        </developers>,

    bintrayReleaseOnPublish in ThisBuild := true,
    bintrayOrganization := Some("snowflakedb"),
    bintrayCredentialsFile := {
      val user = Properties.envOrNone("JENKINS_BINTRAY_USER")
      if (user.isDefined) {
        val workspace = Properties.envOrElse("WORKSPACE", ".")
        new File(s"$workspace/.bintray")
      } else bintrayCredentialsFile.value
    }
  )

//testGrouping in ItTest := Seq[Group](
//  Group(
//    "aws",
//    (definedTests in ItTest).value,
//    SubProcess(
//      ForkOptions().withEnvVars(Map[String, String]("deployment"->"s3"))
//    )
//  ),
//  Group(
//    "azure",
//    (definedTests in ItTest).value,
//    SubProcess(
//      ForkOptions().withEnvVars(Map[String, String]("deployment"->"azure"))
//    )
//  )
//)