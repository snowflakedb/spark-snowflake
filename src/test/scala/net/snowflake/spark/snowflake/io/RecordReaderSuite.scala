package net.snowflake.spark.snowflake.io

import java.io.ByteArrayInputStream

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.FunSuite

class RecordReaderSuite extends FunSuite{

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


    val recordReader: SFRecordReader = new SFRecordReader(SupportedFormat.JSON)

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

}
