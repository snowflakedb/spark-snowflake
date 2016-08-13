# `spark-snowflakedb`

Snowflake connector library for Spark.

For a manual and more information, see 
**[the official documentation](https://docs.snowflake.net/manuals/user-guide/spark-connector.html)**.

[![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

_Note: this project was originally forked from the 
**[spark-redshift](https://github.com/databricks/spark-redshift)** project.
We would like to acknowledge and thank the developers of that project, 
in particular [Josh Rosen](https://github.com/JoshRosen)._

### Developer notes

#### Running unit tests

Simply run
  
    build/sbt test
    
#### Running integration tests

NOTE: Integration tests are currently only partially supported.

Integration tests have the following requirements:
* Snowflake JDBC driver needs to be located in the /tmp/snowflake_jdbc.jar directory.
* An environment variable `IT_SNOWFLAKE_CONF` needs to be defined,
  pointing to the location of the Snowflake configuration file.
* An example configuration file is provided in `snowflake.conf.example`.

Once these requirements are met, run e.g.
    
    export IT_SNOWFLAKE_CONF=$PWD/snowflake.conf 
    build/sbt it:test
  
#### Running code coverage tests

It's best to use Scala 2.11, as some earlier versions have a bug disallowing
line highlighting.

Example use:

    build/sbt -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 clean coverage test

And with integration tests:

    export IT_SNOWFLAKE_CONF=$PWD/snowflake.conf
    build/sbt -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 \
      clean coverage test it:test

To see the results:      

    firefox $PWD/target/scala-2.11/scoverage-report/index.html
