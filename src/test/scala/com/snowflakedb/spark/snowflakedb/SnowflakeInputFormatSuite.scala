/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2014 Databricks
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
package com.snowflakedb.spark.snowflakedb

import java.io.{DataOutputStream, File, FileOutputStream}

import scala.language.implicitConversions

import com.snowflakedb.spark.snowflakedb.SnowflakeInputFormat._
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkContext

class SnowflakeInputFormatSuite extends FunSuite with BeforeAndAfterAll {

  import SnowflakeInputFormatSuite._

  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", this.getClass.getName)
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

  private def writeToFile(contents: String, file: File): Unit = {
    val bytes = contents.getBytes
    val out = new DataOutputStream(new FileOutputStream(file))
    out.write(bytes, 0, bytes.length)
    out.close()
  }

  /** Escape the strings according to Snowflake format rules */
  private def escape(records: Set[Seq[String]], delimiter: Char): String = {
    records.map { r =>
      r.map { f =>
        var res = f.replace("\"", "\"\"")
        res = "\"" + res + "\""
        res
      }.mkString(delimiter)
    }.mkString("", "\n", "\n")
  }

  private final val KEY_BLOCK_SIZE = "fs.local.block.size"

  private final val TAB = '\t'

  private val records = Set(
    Seq("a\n", DEFAULT_DELIMITER + "b\\"),
    Seq("c", TAB + "d"),
    Seq("\ne", "\\\\f"),
    Seq("g\"", "\"h\"")
  )

  private def withTempDir(func: File => Unit): Unit = {
    val dir = Files.createTempDir()
    dir.deleteOnExit()
    func(dir)
  }

  test("default delimiter") {
    withTempDir { dir =>
      val escaped = escape(records, DEFAULT_DELIMITER)
      writeToFile(escaped, new File(dir, "part-00000"))

      val conf = new Configuration
      conf.setLong(KEY_BLOCK_SIZE, 4)

      val rdd = sc.newAPIHadoopFile(dir.toString, classOf[SnowflakeInputFormat],
        classOf[java.lang.Long], classOf[Array[String]], conf)

      val actual = rdd.values.map(_.toSeq).collect()

      assert(actual.length === records.size)
      assert(actual.toSet === records)
    }
  }
}

object SnowflakeInputFormatSuite {
  implicit def charToString(c: Char): String = c.toString
}
