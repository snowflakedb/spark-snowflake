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

import java.lang

import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import java.io.{BufferedInputStream, InputStream}
import java.nio.charset.Charset

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat

import scala.collection.mutable.ArrayBuffer

class SFCSVInputFormat extends FileInputFormat[java.lang.Long, String] {
  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext
                                 ): RecordReader[lang.Long, String] =
    new S3RecordReader(SupportedFormat.CSV)
}
private[io] class S3RecordReader(format: SupportedFormat = SupportedFormat.CSV) extends RecordReader[java.lang.Long, String] {

  @inline private final val lineFeed: Byte = '\n'
  @inline private final val codecBufferSize = 64 * 1024
  @inline private final val inputBufferSize = 1024 * 1024

  private var codec: Option[CompressionCodec] = None
  private var size: Long = 0
  private var cur: Long = 0

  private var key: Long = 0
  private var value: String = _

  private var inputStream: Option[InputStream] = None
  private var currentChar: Option[Byte] = None


  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val inputSplit = split.asInstanceOf[FileSplit]
    val file = inputSplit.getPath
    val conf = context.getConfiguration
    val compressionCodecs = new CompressionCodecFactory(conf)
    codec = Option(compressionCodecs.getCodec(file))
    val fs = file.getFileSystem(conf)
    size = fs.getFileStatus(file).getLen
    cur = 0

    val reader = new BufferedInputStream(fs.open(file), inputBufferSize)

    inputStream =
      if(codec.isDefined) Option(new BufferedInputStream(codec.get.createInputStream(reader), codecBufferSize))
      else Option(reader)

    currentChar = readChar()

  }

  override def nextKeyValue(): Boolean =
    if (currentChar.isDefined) {
      key = cur
      value = format match {
        case SupportedFormat.CSV =>
          val buff = ArrayBuffer.empty[Byte]
          while(currentChar.isDefined && currentChar.get != lineFeed) {
            buff.append(currentChar.get)
            currentChar = readChar()
          }
          currentChar = readChar()
          new String(buff.toArray, Charset.forName("UTF-8"))

        case SupportedFormat.JSON =>
          throw new UnsupportedOperationException("Not support JSON in current version")
          //todo
          ""
      }
      true
    }
    else false

  private def readChar(): Option[Byte] =
    inputStream match {
      case None => None
      case Some(reader) =>
        val c = reader.read().toByte
        cur += 1

        if(c == -1) None else Some(c.toByte)//get negative value sometime
    }



  override def getCurrentKey: lang.Long = key

  override def getCurrentValue: String = value

  override def getProgress: Float =
    if(currentChar.isEmpty) 1.0f else math.min(cur.toFloat / size, 1.0f)

  override def close(): Unit = inputStream.foreach(_.close())
}

