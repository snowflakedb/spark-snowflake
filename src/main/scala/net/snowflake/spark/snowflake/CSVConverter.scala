/*
 * Copyright 2015-2018 Snowflake Computing
 * Copyright 2015 TouchType Ltd
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

import org.apache.spark.sql.types.StructType
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object CSVConverter{

  private final val delimiter = '|'
  private final val quoteChar = '"'
  
  private[snowflake] def convert[T: ClassTag](
                                               partition: Iterator[String],
                                               resultSchema: StructType
                                             ): Iterator[T] = {
    val converter = Conversions.createRowConverter[T](resultSchema)
    partition.map(s=>{
      val fields = ArrayBuffer.empty[String]
      var buff = new StringBuilder

      def addField(): Unit = {
        if (buff.isEmpty) fields.append(null)
        else {
          val field = buff.toString()
          buff = new StringBuilder
          fields.append(field)
        }
      }

      var escaped = false
      var index = 0

      while (index < s.length) {
        escaped = false
        if (s(index) == quoteChar) {
          index += 1
          while (index < s.length && !(escaped && s(index) == delimiter)) {
            if (escaped) {
              escaped = false
              buff.append(s(index))
            }
            else if (s(index) == quoteChar) escaped = true
            else buff.append(s(index))
            index += 1
          }
          addField()
        }
        else {
          while (index < s.length && s(index) != delimiter) {
            buff.append(s(index))
            index += 1
          }
          addField()
        }
        index += 1
      }
      addField()
      converter(fields.toArray)
    })
  }

}
