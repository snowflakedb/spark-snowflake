# `spark-snowflakedb`

Snowflake connector library for Spark.

For a manual and more information, see 
**[the official documentation](https://docs.snowflake.net/manuals/user-guide/spark-connector.html)**.

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
    
    IT_SNOWFLAKE_CONF=$PWD/snowflake.conf build/sbt it:test
  


