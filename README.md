# `spark-snowflake`

## Snowflake Data Source for Apache Spark.
![Build Status](https://github.com/snowflakedb/spark-snowflake/workflows/Spark%20Connector%20Integration%20Test/badge.svg)
[![codecov](https://codecov.io/gh/snowflakedb/spark-snowflake/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/spark-snowflake)
[![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

The main version of `spark-snowflake` works with Spark 2.4. For use with Spark 2.3 and 2.2, please use tag `vx.x.x-spark_2.3` and `vx.x.x-spark_2.2`. 

To use it, provide the dependency for Spark in the form of `net.snowflake:spark-snowflake_$SCALA_VERSION:$RELEASE`, e.g. `net.snowflake:spark-snowflake_2.11:2.2.2`. See [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cspark-snowflake) for more info.

For a version working with Spark 1.5 and 1.6, please use `branch-1.x`. Artifacts of that version are also available in the Snowflake UI.

For a user manual and more information, see 
**[the official documentation](https://docs.snowflake.net/manuals/user-guide/spark-connector.html)**.

For developer notes, see [README-DEV](README-DEV.md)

#### Acknowledgments

This project was originally forked from the 
**[spark-redshift](https://github.com/databricks/spark-redshift)** project.
We also occasionally port relevant patches from it.
We would like to acknowledge and thank the developers of that project, 
in particular [Josh Rosen](https://github.com/JoshRosen).
