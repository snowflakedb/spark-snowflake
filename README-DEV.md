# `Developer notes for spark-snowflakedb`

## Testing

### Running unit tests

Simply run
  
    build/sbt test
    
### Running integration tests

NOTE: Integration tests are currently only partially supported.

Notes about integration tests
* Snowflake JDBC driver needs to be located in the /tmp/snowflake_jdbc.jar directory.
* Config needs to be set with variable `IT_SNOWFLAKE_CONF` defined and
    pointing to the location of the Snowflake configuration file.
* An example configuration file is provided in `snowflake.conf.example`.
* We run with Spark at least 1.6.2 as we test some features not available before

Once these requirements are met, run e.g.
    
    export IT_SNOWFLAKE_CONF_CONTENT=`cat $PWD/snowflake.conf | base64 -w 0` 
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

### Setting up Travis credentials

* Follow initial setup as described in the [Travis manual](https://docs.travis-ci.com/user/encrypting-files/).
* Encrypt your config file
     
      travis encrypt-file snowflake.travis.conf
      
* Add the output line to .travis.yml e.g.

      before_install:
      - openssl aes-256-cbc -K $encrypted_XX_key -iv $encrypted_XXX_iv -in snowflake.travis.conf.enc -out snowflake.travis.conf -d

### Running dev/run-tests-travis.sh manually

Travis uses the `dev/run-tests-travis.sh` script to run its test.

To test its behavior locally, run e.g. this:

    export PATH=$PATH:$PWD/build/ HADOOP_VERSION="2.2.0" SPARK_VERSION="1.6.2" SPARK_SCALA_VERSION="2.11.7" TRAVIS_SCALA_VERSION="2.11.7" 
    ./dev/run-tests-travis.sh
    INTEGRATION_TESTS=true ./dev/run-tests-travis.sh
    