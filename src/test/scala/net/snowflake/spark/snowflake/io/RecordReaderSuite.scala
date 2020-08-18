package net.snowflake.spark.snowflake.io

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.util.zip.GZIPOutputStream

import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.scalatest.FunSuite

class RecordReaderSuite extends FunSuite {

  val mapper: ObjectMapper = new ObjectMapper()

  test("Read Json File") {
    val record1 =
      s"""
         |{
         |  "name":"abc",
         |  "age":123,
         |  "car":[
         |    {
         |      "make": "vw",
         |      "mode": "golf",
         |      "year": 2010
         |    },
         |    {
         |      "make": "Audi",
         |      "mode": "R8",
         |      "year": 2011
         |    }
         |  ]
         |}
         """.stripMargin
    val record2 =
      s"""
         |{
         |  "name":"def ghi",
         |  "age":222,
         |  "car":[
         |    {
         |      "make": "Tesla",
         |      "mode": "X",
         |      "year": 2017
         |    }
         |  ]
         |}
       """.stripMargin
    val file = record1 + record2

    val recordReader: SFRecordReader = new SFJsonInputFormat()
      .createRecordReader(new FileSplit(),
        new TaskAttemptContextImpl(new Configuration(),
        new TaskAttemptID()))
      .asInstanceOf[SFRecordReader]

    recordReader.addStream(new ByteArrayInputStream(file.getBytes))

    recordReader.addStream(new ByteArrayInputStream(file.getBytes))

    val result1 = mapper.readTree(recordReader.next())
    val json1 = mapper.readTree(record1)

    assert(json1.equals(result1))

    val result2 = mapper.readTree(recordReader.next())
    val json2 = mapper.readTree(record2)

    assert(json2.equals(result2))

    val result3 = mapper.readTree(recordReader.next())

    assert(json1.equals(result3))

    val result4 = mapper.readTree(recordReader.next())

    assert(json2.equals(result4))

    assert(!recordReader.hasNext)

  }

  test("test read CSV file") {
    // First file is uncompressed file
    val temp_file1 = File.createTempFile("test_file_", ".csv")
    val temp_file_full_name1 = temp_file1.getPath
    // second file is compressed file
    val temp_file2 = File.createTempFile("test_file_", ".csv.gz")
    val temp_file_full_name2 = temp_file2.getPath
    val data = "Mike,100,\nBob,200,\nJohn,300,\n"

    try {
      // write file1 for uncompressed data
      FileUtils.write(temp_file1, data)

      // write file2 for compressed data
      val gzOutStream = new GZIPOutputStream(new FileOutputStream(temp_file2))
      gzOutStream.write(data.getBytes)
      gzOutStream.close()

      var fileSplit = new FileSplit(new Path(temp_file_full_name1), 0, data.length, null)
      val context: TaskAttemptContext =
        new TaskAttemptContextImpl(new Configuration(),
          new TaskAttemptID())
      val recordReader: SFRecordReader = new SFCSVInputFormat()
      .createRecordReader(fileSplit, context)
          .asInstanceOf[SFRecordReader]

      // initialize with file1, and test some functions.
      recordReader.initialize(fileSplit, context)
      var rowCount = 0
      while (recordReader.nextKeyValue()) {
        val key = recordReader.getCurrentKey
        val value = recordReader.getCurrentValue
        val progress = recordReader.getProgress
        rowCount += 1
        println(s"$rowCount $key $progress $value")
      }
      assert(rowCount == 3)
      recordReader.close()

      // Initialize with file1
      fileSplit = new FileSplit(new Path(temp_file_full_name2), 0, data.length, null)
      recordReader.initialize(fileSplit, context)
      // test close
      recordReader.close()
    } finally {
      FileUtils.deleteQuietly(temp_file1)
      FileUtils.deleteQuietly(temp_file2)
    }
  }
}
