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

import scala.util.Properties

val sparkVersion = settingKey[String]("Spark version")

/*
 * Don't change the variable name "sparkConnectorVersion" because
 * jenkins job "BumpUpSparkConnectorVersion" depends on it.
 * If it has to be changed, please also change the script:
 * Tests/jenkins/BumpUpSparkConnectorVersion/run.sh
 * in snowflake repository.
 */
val sparkConnectorVersion = "3.1.8"

// Extract major.minor from a version string like "3.5.0" -> "3.5"
def sparkMajorMinor(version: String): String = {
  val parts = version.split('.')
  if (parts.length >= 2) s"${parts(0)}.${parts(1)}" else version
}

// Extract major version from a version string like "3.5.0" -> 3
def sparkMajor(version: String): Int = {
  scala.util.Try(version.takeWhile(_ != '.').toInt).getOrElse(3)
}

def versionedSourceDirs(base: File, sparkVer: String): Seq[File] = {
  val mm = sparkMajorMinor(sparkVer)
  val common = Seq(base / "scala")
  if (sparkMajor(sparkVer) >= 4) {
    common ++ Seq(base / "4.x" / "scala", base / mm / "scala")
  } else {
    common :+ (base / mm / "scala")
  }
}

def specialJvmOptions(sparkVer: String): Seq[String] = {
  if (sparkVer >= "4.0.0") {
    Seq(
      "base/java.lang", "base/java.lang.invoke", "base/java.lang.reflect",
      "base/java.io", "base/java.net", "base/java.nio",
      "base/java.util", "base/java.util.concurrent", "base/java.util.concurrent.atomic",
      "base/sun.nio.ch", "base/sun.nio.cs", "base/sun.security.action",
      "base/sun.util.calendar", "security.jgss/sun.security.krb5"
    ).map("--add-opens=java." + _ + "=ALL-UNNAMED")
  } else {
    Seq()
  }
}

lazy val ItTest = config("it") extend Test

// Test to use self-download or self-build JDBC driver
// unmanagedJars in Compile += file(s"lib/snowflake-jdbc-3.12.12.jar")

lazy val root = project.withId("spark-snowflake").in(file("."))
  .configs(ItTest)
  .settings(inConfig(ItTest)(Defaults.testSettings))
  .settings(Defaults.coreDefaultSettings)
  .settings(Defaults.itSettings)
  .settings(
    name := "spark-snowflake",
    organization := "net.snowflake",

    sparkVersion := System.getProperty("sparkVersion", "3.5.0"),

    version := s"${sparkConnectorVersion}-spark_${sparkMajorMinor(sparkVersion.value)}",

    // Note: String comparison works for our version set (3.5, 4.0, 4.1) but
    // would break for e.g. "3.5.10" vs "3.5.9". Keep versions to X.Y.Z format.
    scalaVersion := {
      if (sparkVersion.value >= "4.1.0") "2.13.17"
      else if (sparkVersion.value >= "4.0.0") "2.13.16"
      else sys.props.getOrElse("SPARK_SCALA_VERSION", "2.12.11")
    },

    crossScalaVersions := {
      if (sparkVersion.value >= "4.0.0") {
        Seq() // Spark 4.x is Scala 2.13 only; no cross compilation
      } else {
        Seq("2.12.11", "2.13.10")
      }
    },

    javacOptions ++= {
      if (sparkVersion.value >= "4.0.0") {
        Seq("-source", "17", "-target", "17")
      } else {
        Seq("-source", "1.8", "-target", "1.8")
      }
    },

    // Version-specific source directories:
    //   common (src/main/scala) is always included
    //   Spark 3.5 builds add src/main/3.5/
    //   Spark 4.x builds add src/main/4.x/ + src/main/4.0/ or src/main/4.1/
    Compile / unmanagedSourceDirectories :=
      versionedSourceDirs((Compile / sourceDirectory).value, sparkVersion.value),

    Test / unmanagedSourceDirectories :=
      versionedSourceDirs((Test / sourceDirectory).value, sparkVersion.value),

    ItTest / unmanagedSourceDirectories :=
      versionedSourceDirs((ItTest / sourceDirectory).value, sparkVersion.value),

    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    // Set up GPG key for release build from environment variable: GPG_HEX_CODE
    // Build jenkins job must have set it, otherwise, the release build will fail.
    credentials += Credentials(
      "GnuPG Key ID",
      "gpg",
      Properties.envOrNone("GPG_HEX_CODE").getOrElse("Jenkins_build_not_set_GPG_HEX_CODE"),
      "ignored" // this field is ignored; passwords are supplied by pinentry
    ),
    libraryDependencies ++= {
      val sv = sparkVersion.value
      Seq(
        "net.snowflake" % "snowflake-jdbc" % "3.28.0",
        "org.scalatest" %% "scalatest" % "3.1.1" % Test,
        "org.mockito" % "mockito-core" % "1.10.19" % Test,
        "org.apache.commons" % "commons-lang3" % "3.18.0" % "provided",
        // For test to read/write from postgresql
        "org.postgresql" % "postgresql" % "42.5.4" % Test,
        "org.apache.spark" %% "spark-core" % sv % "provided, test",
        "org.apache.spark" %% "spark-sql" % sv % "provided, test",
        "org.apache.spark" %% "spark-catalyst" % sv % "provided, test",
        "org.apache.spark" %% "spark-mllib" % sv % "test",
        "org.apache.spark" %% "spark-core" % sv % "provided, test" classifier "tests",
        "org.apache.spark" %% "spark-sql" % sv % "provided, test" classifier "tests",
        "org.apache.spark" %% "spark-catalyst" % sv %
          "provided, test" classifier "tests",
        "org.apache.spark" %% "spark-core" % sv % "provided, test"
          classifier "test-sources",
        "org.apache.spark" %% "spark-sql" % sv % "provided, test"
          classifier "test-sources",
        "org.apache.spark" %% "spark-catalyst" % sv % "provided, test"
          classifier "test-sources",
        "org.apache.parquet" % "parquet-avro" % "1.15.2"
      ) ++ (
        if (sv >= "4.0.0") Seq(
          "org.apache.spark" %% "spark-sql-api" % sv % "provided, test"
        ) else Seq.empty
      )
    },

    // scalac-scoverage-plugin is full-version cross-compiled per Scala patch version.
    // Older Scala versions (2.12.11, 2.13.10) only publish up to 2.0.x,
    // while 2.13.16+ starts at 2.3.0. Pick the latest available per version.
    coverageScalacPluginVersion := {
      if (scalaVersion.value >= "2.13.16") "2.5.2"
      else "2.0.11"
    },

    Compile / scalastyleSources ++= (Compile / unmanagedSourceDirectories).value,
    Test / scalastyleSources ++= (Test / unmanagedSourceDirectories).value,

    Test / testOptions += Tests.Argument("-oF"),
    Test / fork := true,
    Test / javaOptions ++= Seq("-Xms1024M", "-Xmx4096M"),
    Test / javaOptions ++= specialJvmOptions(sparkVersion.value),

    // Release settings
    Global / pgpPassphrase := Properties.envOrNone("GPG_KEY_PASSPHRASE").map(_.toCharArray),

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

    // New setting for the Central Portal
    publishTo := {
      val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
      if (isSnapshot.value) {
        Some("central-snapshots" at centralSnapshots)
      } else {
        localStaging.value
      }
    }

  )
