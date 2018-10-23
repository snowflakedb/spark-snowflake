/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 Databricks
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

import org.scalastyle.sbt.ScalastylePlugin.rawScalastyleSettings
import sbt._
import sbt.Keys._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import scoverage.ScoverageSbtPlugin
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import com.typesafe.sbt.pgp._
import bintray.BintrayPlugin.autoImport._
import scala.util.Properties

object SparkSnowflakeBuild extends Build {
  val testSparkVersion = settingKey[String]("Spark version to test against")

  // Define a custom test configuration so that unit test helper classes can be re-used under
  // the integration tests configuration; see http://stackoverflow.com/a/20635808.
  lazy val IntegrationTest = config("it") extend Test

  lazy val root = Project("spark-snowflake", file("."))
    .configs(IntegrationTest)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(Project.inConfig(IntegrationTest)(rawScalastyleSettings()): _*)
    .settings(Defaults.coreDefaultSettings: _*)
    .settings(Defaults.itSettings: _*)
    .settings(
      name := "spark-snowflake",
      organization := "net.snowflake",
      scalaVersion := sys.props.getOrElse("SPARK_SCALA_VERSION", default = "2.11.12"),
      crossScalaVersions := Seq("2.10.5","2.11.12"),
      sparkVersion := "2.2.0",
      testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value),
      javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
      spName := "snowflake/spark-snowflake",
      sparkComponents ++= Seq("sql"),
      spIgnoreProvided := true,
      licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
      credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
      resolvers +=
        "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      libraryDependencies ++= Seq(
        "net.snowflake" % "snowflake-ingest-sdk" % "0.9.2" excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core")),
        "net.snowflake" % "snowflake-jdbc" % "3.6.15",
        "com.google.guava" % "guava" % "14.0.1" % Test,
        "org.scalatest" %% "scalatest" % "3.0.5" % Test,
        "org.mockito" % "mockito-core" % "1.10.19" % Test,
        "org.apache.commons" % "commons-lang3" % "3.5",

        "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
        "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force(),
        "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "test" force()
      ),
      ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
        if (scalaBinaryVersion.value == "2.10") false
        else true
      },
      logBuffered := false,
      // Display full-length stacktraces from ScalaTest:
      testOptions in Test += Tests.Argument("-oF"),
      fork in Test := true,
      javaOptions in Test ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M"),

      /********************
       * Release settings *
       ********************/
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
        </developers>,

      bintrayReleaseOnPublish in ThisBuild := true,
      bintrayOrganization := Some("snowflakedb"),
      bintrayCredentialsFile := {
        val user = Properties.envOrNone("JENKINS_BINTRAY_USER")
        if (user.isDefined) {
          val workspace = Properties.envOrElse("WORKSPACE", ".")
          new File(s"""$workspace/.bintray""")
        } else bintrayCredentialsFile.value
      },

      // Add publishing to spark packages as another step.
      releaseProcess := Seq[ReleaseStep](
        checkSnapshotDependencies,
        inquireVersions,
        runTest,
        setReleaseVersion,
        commitReleaseVersion,
        tagRelease
      )
      // Snowflake-todo: These are removed just in case for now
//        publishArtifacts,
//        setNextVersion,
//        commitNextVersion,
//        pushChanges

    )
}
