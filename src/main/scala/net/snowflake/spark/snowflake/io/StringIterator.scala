package net.snowflake.spark.snowflake.io

import java.io.InputStream

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat


private[io] class StringIterator(
                                  format: SupportedFormat = SupportedFormat.CSV
                                ) extends Iterator[String]{

  private val lineFeed: Byte = '\n'

  private var inputStreams: List[InputStream] = Nil
  private var currentStream: InputStream = _

  def addStream(stream: InputStream): Unit =
    inputStreams = stream :: Nil

  override def hasNext: Boolean = !(inputStreams.isEmpty && currentStream==null)

  override def next(): String = {
    if(!hasNext) null
    else{
      if(currentStream == null) {
        currentStream = inputStreams.head
        inputStreams = inputStreams.tail
      }
      val buff = new StringBuilder()

      format match {
        case SupportedFormat.CSV =>
          var quotedNum = 0
          var c = nextChar()
          while(c.isDefined && !(quotedNum%2==0 && c.get==lineFeed)){
            c.get match {
              case '\\' =>
                buff.append(c.get)
                c = nextChar()
              case '"' =>
                quotedNum += 1
              case _ =>
            }

            buff.append(c.get)
            c = nextChar()
          }
          if(c.isEmpty) {
            currentStream.close()
            currentStream = null
          }
        case SupportedFormat.JSON =>
          //todo
      }

      buff.toString()
    }
  }

  private def nextChar(): Option[Char] = {
    val c = currentStream.read()
    if(c < 0) None else Some(c.toChar)
  }

  def closeAll(): Unit =
    inputStreams.foreach(_.close())
}
