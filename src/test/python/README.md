# `spark-snowflake Python Test`

## Snowflake Spark Connector python test

Currently, scala unit and integration test are used. For long term, python test needs to be setup also because pyspark is used widely in the big data world.

Snowflake Spark Connector python test has not been setup. The python test framework setup is a separate topic. So far, it is necessary to run some pyspark manual test to verify some customer issues. Below is a sample about that.

unittest.py is a sample program to very Spark Python UDF to work with Snowflake Spark Connector. It defines function getConfig() to retrieve test account info from snowflake.travis.json.

Run tips
=======
1. A apache spark needs to be installed or built to run the python unit test.
   For my case, I make a build from Apache Spark at /Users/mrui/spark/spark
2. The unit test can be run by 'spark-submit' for example
   /Users/mrui/spark/spark/bin/spark-submit --packages net.snowflake:snowflake-jdbc:3.11.0,net.snowflake:spark-snowflake_2.12:2.5.6-spark_2.4 unittest.py

#### Acknowledgments

To-be-filled
