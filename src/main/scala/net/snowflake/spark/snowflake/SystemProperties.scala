package net.snowflake.spark.snowflake

import java.lang.System
import scala.collection.JavaConverters._


object SystemProperties {

	def readParameters() = {
		System.getProperties.asScala.flatMap{ case(key, value) =>
			if (key.toLowerCase.startsWith("snowflake.")) {
				Some((key.toLowerCase.replace("snowflake.", ""), value.toLowerCase))
			} else {
				None
			}
		}
	}

}