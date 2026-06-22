/*
 * Copyright 2015-2026 Snowflake Computing
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

import java.io.File

import scala.io.Source

import org.scalatest.funsuite.AnyFunSuite

/**
  * Guards the cross-Spark publish loop in deploy.sh against two regressions that
  * each broke the v3.2.0 release (MavenPushSparkConnector #51/#52/#53):
  *
  *   1. Staging not cleared between iterations. sbt 1.11's built-in sona publish
  *      stages under target/sona-staging; if it is not cleared between Spark
  *      versions, sonaUpload bundles previously-published artifacts and Maven
  *      Central rejects the deployment as duplicates.
  *
  *   2. Using +publishSigned for Spark 4. Spark 4.x sets crossScalaVersions to
  *      empty (Scala 2.13 only), so the + prefix iterates over nothing and
  *      stages no artifacts, producing an empty bundle. Spark 4 must publish
  *      without +.
  */
class DeployScriptSuite extends AnyFunSuite {

  private def deployScript: String = {
    val candidates = Seq(
      new File("deploy.sh"),
      new File(System.getProperty("user.dir"), "deploy.sh")
    )
    val found = candidates.find(_.isFile).getOrElse {
      fail(
        "deploy.sh not found from working directory " +
          s"'${System.getProperty("user.dir")}'. Tried: " +
          candidates.map(_.getAbsolutePath).mkString(", ")
      )
    }
    val src = Source.fromFile(found)
    try src.mkString
    finally src.close()
  }

  test("deploy.sh publish loop clears the sona staging dir between iterations") {
    val cleanupLines = deployScript.linesIterator
      .map(_.trim)
      .filter(line => line.startsWith("rm -rf") && line.contains("staging"))
      .toSeq

    assert(
      cleanupLines.nonEmpty,
      "deploy.sh has no `rm -rf ...staging...` cleanup. The cross-Spark publish " +
        "loop must clear the local staging dir between iterations, otherwise " +
        "sonaUpload bundles artifacts from earlier Spark versions and Maven " +
        "Central rejects the deployment as duplicates."
    )

    assert(
      cleanupLines.exists(_.contains("sona-staging")),
      "deploy.sh must clean target/sona-staging between publish iterations " +
        "(that is where sbt's built-in sona publish stages artifacts, per " +
        "`show localStaging`). Found cleanup lines: " + cleanupLines.mkString("; ")
    )
  }

  test("deploy.sh publishes Spark 4 without the + cross-build prefix") {
    val publishLines = deployScript.linesIterator
      .map(_.trim)
      .filter(line => line.startsWith("sbt ") && line.contains("publishSigned"))
      .toSeq

    assert(
      publishLines.exists(_.contains("+publishSigned")),
      "Expected a +publishSigned invocation for Spark 3.x (cross-builds Scala " +
        "2.12 + 2.13). Found: " + publishLines.mkString("; ")
    )

    assert(
      publishLines.exists(line => line.contains(" publishSigned")),
      "Expected a plain (non-+) publishSigned invocation for Spark 4.x, whose " +
        "empty crossScalaVersions makes +publishSigned a no-op that stages an " +
        "empty bundle. Found: " + publishLines.mkString("; ")
    )
  }
}
