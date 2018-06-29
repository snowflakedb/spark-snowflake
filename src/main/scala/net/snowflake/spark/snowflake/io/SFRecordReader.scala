package net.snowflake.spark.snowflake.io

import java.io.{BufferedInputStream, InputStream}
import java.lang
import java.nio.charset.Charset

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import scala.collection.mutable.ArrayBuffer


class SFInputFormat extends FileInputFormat[java.lang.Long, String] {
  override def createRecordReader(
                                   split: InputSplit,
                                   context: TaskAttemptContext
                                 ): RecordReader[lang.Long, String] =
    new SFRecordReader(SupportedFormat.CSV)
}

private[io] class SFRecordReader(val format: SupportedFormat = SupportedFormat.CSV)
  extends RecordReader[java.lang.Long, String]
  with Iterator[String]{

  @inline private final val lineFeed: Byte = '\n'
  @inline private final val quoteChar: Byte = '"'
  @inline private final val codecBufferSize = 64 * 1024
  @inline private final val inputBufferSize = 1024 * 1024

  private var inputStreams: List[InputStream] = Nil
  private var currentStream: Option[InputStream] = None
  private var currentChar: Option[Byte] = None

  //for recordReader only
  private var codec: Option[CompressionCodec] = None
  private var fileSize: Long = 0
  private var cur: Long = 0

  private var key: Long = 0
  private var value: String = _



  def addStream(stream: InputStream): Unit =
    inputStreams = stream :: inputStreams


  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val inputSplit = split.asInstanceOf[FileSplit]
    val file = inputSplit.getPath
    val conf = context.getConfiguration
    val compressionCodecs = new CompressionCodecFactory(conf)
    codec = Option(compressionCodecs.getCodec(file))
    val fs = file.getFileSystem(conf)
    fileSize = fs.getFileStatus(file).getLen
    cur = 0
    val reader = new BufferedInputStream(fs.open(file), inputBufferSize)

    inputStreams =
      {
        if(codec.isDefined)
          new BufferedInputStream(codec.get.createInputStream(reader), codecBufferSize)
        else reader
      } :: inputStreams

  }

  /**
    * Only this method updates the value of current key and value, next() method
    * doesn't.
    * @return False if no more record, True otherwise
    */
  override def nextKeyValue(): Boolean = {
    if(hasNext) {
      key = cur
      value = next()
      true
    } else false
  }

  override def getCurrentKey: lang.Long = key

  override def getCurrentValue: String = value

  override def getProgress: Float =
    if(hasNext) math.min(cur.toFloat / fileSize, 1.0f) else 1.0f

  /**
    * In case of exceptions, using this method to manually close input streams.
    */
  override def close(): Unit = inputStreams.foreach(_.close())

  override def hasNext: Boolean =
    inputStreams.nonEmpty || currentStream.isDefined

  override def next(): String = {
    if (!hasNext) null
    else {
      if (currentStream.isEmpty) nextStream()
      val buff = ArrayBuffer.empty[Byte]
      format match {
        case SupportedFormat.CSV =>
          var numOfQuote: Int = 0
          if (currentChar.isEmpty) currentChar = readChar()
          if (currentChar.get == quoteChar) numOfQuote += 1

          while (currentChar.isDefined &&
            !(currentChar.get == lineFeed && numOfQuote % 2 == 0)) {
            buff.append(currentChar.get)
            currentChar = readChar()
            if(currentChar.get == quoteChar) numOfQuote += 1
          }
          currentChar = readChar()

          if(currentChar.isEmpty) nextStream()

        case SupportedFormat.JSON =>
          throw new UnsupportedOperationException("Not Support JSON in current version")
          //todo
      }
      new String(buff.toArray, Charset.forName("UTF-8"))
    }
  }

  /**
    * read a char from current stream
    * @return an optional char value, None for empty
    */
  private def readChar(): Option[Byte] = {
    if(currentStream.isEmpty) None
    else {
      val c = currentStream.get.read()
      cur += 1
      if (c == -1) None else Some(c.toByte) // get negative value sometime
    }
  }

  /**
    * switch currentStream to next inputStream in the stream list, None if empty
    * stream list.
    */
  private def nextStream(): Unit = {
    if(currentStream.isDefined) currentStream.get.close()
    if(inputStreams.nonEmpty){
      currentStream = Some(inputStreams.head)
      inputStreams = inputStreams.tail
    } else{
      currentStream = None
    }
    currentChar = None
  }
}
