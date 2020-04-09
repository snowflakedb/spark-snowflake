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

val sparkVersion = "2.4.0"
val testSparkVersion = sys.props.get("spark.testVersion").getOrElse(sparkVersion)

lazy val root = project.withId("spark-snowflake").in(file("."))
  .settings(
    name := "ClusterTest",
    organization := "net.snowflake",
    version := s"1.0",
    scalaVersion := sys.props.getOrElse("SPARK_SCALA_VERSION", default = "2.12.8"),
    crossScalaVersions := Seq("2.11.12", "2.12.8"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    resolvers +=
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    libraryDependencies ++= Seq(
      "net.snowflake" % "snowflake-ingest-sdk" % "0.9.6",
      "net.snowflake" % "snowflake-jdbc" % "3.12.2",
      "net.snowflake" %% "spark-snowflake" % "2.7.0-spark_2.4",
      // "com.google.guava" % "guava" % "14.0.1" % Test,
      // "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      // "org.mockito" % "mockito-core" % "1.10.19" % Test,
      "org.apache.commons" % "commons-lang3" % "3.5" % "provided",
      // Below is for Spark Streaming from Kafka test only
      // "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
      "org.apache.spark" %% "spark-core" % testSparkVersion % "provided, test",
      "org.apache.spark" %% "spark-sql" % testSparkVersion % "provided, test",
      "org.apache.spark" %% "spark-hive" % testSparkVersion % "provided, test"
    ),
  )
