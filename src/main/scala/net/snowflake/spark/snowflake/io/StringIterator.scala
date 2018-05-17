package net.snowflake.spark.snowflake.io

import java.io.InputStream

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat


private[io] class StringIterator(
                                  format: SupportedFormat = SupportedFormat.CSV
                                ) extends Iterator[String] {

  private val lineFeed: Byte = '\n'

  private var inputStreams: List[InputStream] = Nil
  private var currentStream: InputStream = _
  private var currentChar: Option[Char] = None

  def addStream(stream: InputStream): Unit =
    inputStreams = stream :: Nil

  override def hasNext: Boolean = !(inputStreams.isEmpty && currentStream == null)

  override def next(): String = {
    if (!hasNext) null
    else {
      if (currentStream == null) {
        currentStream = inputStreams.head
        inputStreams = inputStreams.tail
        currentChar = None
      }
      val buff = new StringBuilder()

      format match {
        case SupportedFormat.CSV =>
          var quotedNum = 0
          if (currentChar.isEmpty) currentChar = nextChar()
          while (currentChar.isDefined && !(quotedNum % 2 == 0 && currentChar.get == lineFeed)) {
            currentChar match {
              case Some('\\') =>
                buff.append(currentChar.get)
                currentChar = nextChar()
              case Some('"') =>
                quotedNum += 1
              case _ =>
            }

            buff.append(currentChar.get)
            currentChar = nextChar()
          }
          currentChar = nextChar()
          if (currentChar.isEmpty) {
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
    if (c < 0) None else Some(c.toChar)
  }

  def closeAll(): Unit =
    inputStreams.foreach(_.close())
}
