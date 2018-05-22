/*
 * Copyright 2018 Snowflake Computing
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

import java.io.InputStream
import java.nio.charset.Charset

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat

import scala.collection.mutable.ArrayBuffer


private[io] class StringIterator(
                                  format: SupportedFormat = SupportedFormat.CSV
                                ) extends Iterator[String] {

  private val lineFeed: Byte = '\n'
  private val quoteChar: Byte = '"'

  private var inputStreams: List[InputStream] = Nil
  private var currentStream: Option[InputStream] = None
  private var currentChar: Option[Byte] = None

  def addStream(stream: InputStream): Unit =
    inputStreams = stream :: Nil

  override def hasNext: Boolean = inputStreams.nonEmpty || currentStream.isDefined

  override def next(): String = {
    if (!hasNext) null
    else {
      if (currentStream.isEmpty) {
        currentStream = Some(inputStreams.head)
        inputStreams = inputStreams.tail
        currentChar = None
      }
      val buff = ArrayBuffer.empty[Byte]

      format match {
        case SupportedFormat.CSV =>
          if (currentChar.isEmpty) currentChar = nextChar()

          while (currentChar.isDefined && currentChar.get != lineFeed) {
            buff.append(currentChar.get)
            currentChar = nextChar()
          }
          currentChar = nextChar()
          if (currentChar.isEmpty) {
            currentStream.foreach(_.close())
            currentStream = None
          }
        case SupportedFormat.JSON =>
          throw new UnsupportedOperationException("Not support JSON in current version")
        //todo
      }
      new String(buff.toArray, Charset.forName("UTF-8"))
    }
  }

  private def nextChar(): Option[Byte] = {
    val c = currentStream.get.read()
    if (c < 0) None else Some(c.toByte)
  }

  def closeAll(): Unit =
    inputStreams.foreach(_.close())
}
