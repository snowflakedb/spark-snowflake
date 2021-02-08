# `Developer notes for spark-snowflakedb`

## Building

To build your own .jar, simply issue (adapt your Scala version if needd)

    $ sbt -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 package
     
This will create spark-snowflakedb*.jar that you can use with Spark.


## Testing

### Running unit tests

Simply run
  
    sbt test
    
### Running integration tests

NOTE: Integration tests are currently only partially supported.

Notes about integration tests
* Config needs to be set with variable `IT_SNOWFLAKE_CONF` defined and
    pointing to the location of the Snowflake configuration file.
* An example configuration file is provided in `snowflake.conf.example`.
* We run with Spark at least 2.0.0

Once these requirements are met, run e.g.
    
    export IT_SNOWFLAKE_CONF=$PWD/snowflake.conf 
    sbt -Dspark.version=2.4.0 it:test
  
### Running code coverage tests

It's best to use Scala 2.11, as some earlier versions have a bug disallowing
line highlighting.

Example use:

    sbt -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 clean coverage test

And with integration tests:

    export IT_SNOWFLAKE_CONF=$PWD/snowflake.conf
    sbt -Dspark.version=2.0.0 -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 \
      clean coverage test it:test

To see the results:      

    firefox $PWD/target/scala-2.11/scoverage-report/index.html
    
