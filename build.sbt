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
import ReleaseTransformations._
import ScalastylePlugin.rawScalastyleSettings
import scala.util.Properties

val sparkVersion = "2.4.0"
val testSparkVersion = sys.props.get("spark.testVersion").getOrElse(sparkVersion)

// Define a custom test configuration so that unit test helper classes can be re-used under
// the integration tests configuration; see http://stackoverflow.com/a/20635808.
lazy val IntegrationTest = config("it") extend Test

lazy val root = project.in(file("."))
  .configs(IntegrationTest)
  .settings(Project.inConfig(IntegrationTest)(rawScalastyleSettings()))
  .settings(Defaults.coreDefaultSettings)
  .settings(Defaults.itSettings)
  .settings(
    name := "spark-snowflake",
    organization := "net.snowflake",
    scalaVersion := sys.props.getOrElse("SPARK_SCALA_VERSION", default = "2.11.12"),
    crossScalaVersions := Seq("2.11.12"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    resolvers +=
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    libraryDependencies ++= Seq(
      "net.snowflake" % "snowflake-ingest-sdk" % "0.9.5",
      "net.snowflake" % "snowflake-jdbc" % "3.6.15",
      "com.google.guava" % "guava" % "14.0.1" % Test,
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.mockito" % "mockito-core" % "1.10.19" % Test,
      "org.apache.commons" % "commons-lang3" % "3.5",

      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,

      "org.apache.spark" %% "spark-core" % testSparkVersion % Test force(),
      "org.apache.spark" %% "spark-sql" % testSparkVersion % Test force(),
      "org.apache.spark" %% "spark-hive" % testSparkVersion % Test force(),
    ),
    coverageHighlighting := {
      if (scalaBinaryVersion.value == "2.10") false
      else true
    },
    logBuffered := false,
    // Display full-length stacktraces from ScalaTest:
    Test / testOptions += Tests.Argument("-oF"),
    Test / fork := true,
    Test / javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M"),

    /********************
     * Release settings *
     ********************/
    usePgpKeyHex(Properties.envOrElse("GPG_SIGNATURE", "12345")),
    Global / pgpPassphrase := Properties.envOrNone("GPG_KEY_PASSPHRASE").map(pw => pw.toCharArray),

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

    ThisBuild / bintrayReleaseOnPublish := true,
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
    ),
    // Snowflake-todo: These are removed just in case for now
//        publishArtifacts,
//        setNextVersion,
//        commitNextVersion,
//        pushChanges

  )
