# `Developer notes for spark-snowflakedb`

### Running unit tests

Simply run
  
    build/sbt test
    
### Running integration tests

NOTE: Integration tests are currently only partially supported.

Notes about integration tests
* Snowflake JDBC driver needs to be located in the /tmp/snowflake_jdbc.jar directory.
* An environment variable `IT_SNOWFLAKE_CONF` needs to be defined,
  pointing to the location of the Snowflake configuration file.
* An example configuration file is provided in `snowflake.conf.example`.
* We run with Spark at least 1.6.2 as we test some features not available before

Once these requirements are met, run e.g.
    
    export IT_SNOWFLAKE_CONF=$PWD/snowflake.conf 
    build/sbt -Dspark.version=1.6.2 it:test
  
### Running code coverage tests

It's best to use Scala 2.11, as some earlier versions have a bug disallowing
line highlighting.

Example use:

    build/sbt -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 clean coverage test

And with integration tests:

    export IT_SNOWFLAKE_CONF=$PWD/snowflake.conf
    build/sbt -Dspark.version=1.6.2 -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 \
      clean coverage test it:test

To see the results:      

    firefox $PWD/target/scala-2.11/scoverage-report/index.html

