package net.snowflake.spark.snowflake

import java.text.SimpleDateFormat

class SnowflakeStreamingReport(
                                val stageName: String,
                                val tempDir: Option[String] = None,
                                val keepFailedFile: Boolean = false
                              ) {
  val DATE_FORMAT = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z")

  val startTime = System.currentTimeMillis()

  lazy val endTime: Long = System.currentTimeMillis()

  var fileList: List[String] = Nil

  def addFailedFiles(files: List[String]): Unit =
    fileList = files ::: fileList

  def dropStage: Boolean =
    tempDir.isDefined || !keepFailedFile || fileList.isEmpty


  private def failedFiles : String = {

    def location: String =
      tempDir match {
        case Some(str) => s"File Location: $str"
        case None => s"Files Location: Stage $stageName"
      }

    if (fileList.isEmpty) "All staging files were successfully loaded to Snowflake"
    else
      s"""
         |Failed Files: ${fileList.mkString(" , ")}
         |$location
         |""".stripMargin
  }

  private def time : String = {
    var total = endTime - startTime
    val days = total / (1000*60*60*24)
    total %= 1000*60*60*24
    val hours = total / (1000*60*60)
    total %= 1000*60*60
    val mins = total / (1000*60)
    total %= 1000*60
    val secs = total / 1000
    val ms = total % 1000

    s"""
       |Start Time: ${DATE_FORMAT.format(startTime)}
       |End Time: ${DATE_FORMAT.format(endTime)}
       |Total: $days days $hours hours $mins mins $secs.$ms secs
     """.stripMargin
  }

  override def toString: String =
    s"""
       |Streaming Task Report
       |$time
       |${if(keepFailedFile) failedFiles else ""}
     """.stripMargin

}
