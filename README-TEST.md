# `Developer notes for spark-snowflakedb CI/CD test framework`

## CI/CD test framework introduction

Github Action is used for the CI/CD test. The workflows can be found in `.github/workflows`. Currently, there are two workflows:
* `IntegrationTest.yml`: it builds spark connector binary and runs integration test. This test runs with ONE node local spark cluster. It is the main test suite.
* `ClusterTest.yml`: it builds spark connector binary, setup a spark cluster with docker container and submit test job to the cluster for execution. This test runs with a real spark cluster. It's additional for IntegrationTest.yml.

### Manage secrets
To run integration and cluster test, Snowflake login credentials are needed. The login credentials are stored in an encrypted json file: `snowflake.travis.json.gpg`. The decryption passphrase is stored in github secret settings named `SNOWFLAKE_TEST_CONFIG_SECRET`. The workflow will decrypt the json file for testing. For more details refer to [Creating and storing encrypted secrets](https://help.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets).

An example to retrieve the secret:

    - name: Decrypt snowflake.json for testing
      run: ./.github/scripts/decrypt_secret.sh snowflake.travis.json snowflake.travis.json.gpg
      env:
        SNOWFLAKE_TEST_CONFIG_SECRET: ${{ secrets.SNOWFLAKE_TEST_CONFIG_SECRET }}

## IntegrationTest
Test workflow file is .github/workflows/IntegrationTest.yml. Most of the development tests are included in this workflow. Its steps are:
* Decrypt snowflake.travis.json
* Run IT test with script: .github/scripts/run-tests-github.sh
* Update test coverage report 

## ClusterTest
Test workflow file is .github/workflows/ClusterTest.yml. Comparing with IntegrationTest, this workflow is more complex. It needs below steps.
* Build docker image
* Start spark cluster and run test
* Check test result
* Clean up

### Build docker image
This step create a spark base image with apache spark, spark connector binary, test binaries and dependent libraries. Related files:
* .github/docker/Dockerfile is the docker file. Many arguments are introduced for this docker file to support multiple versions.
* .github/docker/build_image.sh is the script to create the image.

High level steps to build the docker image:
* Build spark connector binary
* Build scala test jar file and achieve the jar and Python test files as a *.tar.gz file.
* Build docker image with required arguments including spark download URL, JDBC download URL, spark connector binary, decrypt script, encrypted test json file, etc.

Example command in repository root directory:  

    .github/docker/build_image.sh

### Start spark cluster and run test
docker-compose is used to start a spark cluster and run test. There is one master node and two worker nodes in the cluster. The test driver also runs as a docker-compose service. The test driver script is setup when the docker image is built by RUN_TEST_SCRIPT. As for ClusterTest, the script is ClusterTest/run_cluster_test.sh. For more details refer to .github/docker/docker-compose.yml.

Example command in repository root directory:  

  
    docker-compose -f .github/docker/docker-compose.yml up [-d]
    
### Check test result
In above step, the docker-compose starts the services with "-d". This step waits for the test done and check the result.

Github Action provides global environment variable `GITHUB_RUN_ID` which is unique in a repository. The test check is based on it. NOTE: Re-run doesn't change it. `GITHUB_SHA` is the commit ID.

A snowflake table: CLUSTER_TEST_RESULT_TABLE is set up to store the result for test case. Column: GITHUBRUNID in this table is for GITHUB_RUN_ID. Column: TESTSTATUS is for test status which can be Fail/Success/Exception. Column: REASON is to indicate details.

For each test case, it must write one row into CLUSTER_TEST_RESULT_TABLE when the test case is done.

For each test run, the total test case count is known. So, we check the finished test count with this GITHUB_RUN_ID. The test run is regarded to be done if the finished test case count is equal to the total test case count. In addition, wait-for timeout is introduced to make sure the test run can be done.

After the test is done, if the Success test case count is equal to the total test case count, the test run is regarded as Success, otherwise, it fails.

Example command in repository root directory:  
  
    .github/docker/check_result.sh

### Clean up
It kills and removes the docker containers, remove the docker image and so on.

Example command in repository root directory:  
  
    .github/docker/cleanup_docker.sh

## Cluster Test Suite Framework
The cluster test cases are developed in directory: `ClusterTest`. Both Python and Scala are developed. Scala tests are mainly used.

### Python test
There is only one test case: ClusterTest/src/main/python/ClusterTest.py

Two Example commands in repository root directory:  
  
    # run the job in local
    $SPARK_HOME/bin/spark-submit \
      --conf "spark.pyspark.python=python3" \
      --conf "spark.pyspark.driver.python=python3" \
      --jars $SPARK_WORKDIR/spark-snowflake_2.11-2.7.0-spark_2.4.jar,$SPARK_WORKDIR/snowflake-jdbc-3.12.2.jar \
      ClusterTest/src/main/python/ClusterTest.py local

    # run the job in remote cluster
    $SPARK_HOME/bin/spark-submit \
      --conf "spark.pyspark.python=python3" \
      --conf "spark.pyspark.driver.python=python3" \
      --jars $SPARK_WORKDIR/spark-snowflake_2.11-2.7.0-spark_2.4.jar,$SPARK_WORKDIR/snowflake-jdbc-3.12.2.jar \
      --master spark://master:7077 --deploy-mode client \
      $SPARK_WORKDIR/ClusterTest.py remote

### Scala Test
Scala Test is well-designed to add more test cases in the future. There is a sbt project in ClusterTest for the scala test. IntelliJ can import this project separately.

Example command to compile the test project  

    cd ClusterTest
    sbt ++2.11.12 package
    cd ..

The test driver class is net.snowflake.spark.snowflake.ClusterTest . The caller can run multiple classes (multiple test cases are separated by ';').

Example command to run the scala test  

    # run the job in local
    $SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/spark-snowflake_2.11-2.7.0-spark_2.4.jar,$SPARK_WORKDIR/snowflake-jdbc-3.12.2.jar \
      --class net.snowflake.spark.snowflake.ClusterTest \
      ClusterTest/target/scala-2.11/clustertest_2.11-1.0.jar \
      local "net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite;"

    # run the job in remote cluster
    $SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/spark-snowflake_2.11-2.7.0-spark_2.4.jar,$SPARK_WORKDIR/snowflake-jdbc-3.12.2.jar \
      --master spark://master:7077 --deploy-mode client \
      --class net.snowflake.spark.snowflake.ClusterTest \
      ClusterTest/target/scala-2.11/clustertest_2.11-1.0.jar \
      remote "net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite;net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite;"

### How to add a new test case
net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite can be a good template to add a new test case. Below are suggested operations.
* Create a new class in the same directory for example ClusterTest/src/main/scala/net/snowflake/spark/snowflake/testsuite/MyNewTestSuite.scala
* This class must inherit from `trait ClusterTestSuiteBase`. It needs to implement: `runImpl(SparkSession,ClusterTestResultBuilder)`
* Update ClusterTest/run_cluster_test.sh to run the new test. It can be submitted together with other test case or separately.
* Update TOTAL_TEST_CASE_COUNT in .github/workflows/ClusterTest*.yml to make sure it is correct.

### Test case development tips
IntelliJ can be used to develop the test case. You can set below to debug the test case in IntelliJ.
* Program arguments: "local net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite"
* Environment Variables: "GITHUB_RUN_ID=debug_run_id;GITHUB_SHA=debug_commit_id"

Similarly, you can also debug net.snowflake.spark.snowflake.ClusterTestCheckResult in IntelliJ.
* Program arguments: "2 60"
* Environment Variables: "GITHUB_RUN_ID=debug_run_id;GITHUB_SHA=debug_commit_id"

## Related file short description
Files in directory .github/workflows
* ClusterTest.yml: Workflow definition file for Cluster Test
* IntegrationTest.yml: Workflow definition file for Integration Test

Files in directory .github/docker
* Dockerfile: the docker file for spark base image
* build_image.sh: the script to create spark base image
* check_result.sh: the script to wait for test done and  check test result
* cleanup_docker.sh: the script to clean up container and image.
* docker-compose.yml: the docker-compose file
* entrypoint.sh: the boot script when a docker image run
* logging.properties: the logging properties for JDBC, it is not supported yet.
* spark-env.sh: The env setup script for spark master and worker. It is not used yet.

Files in directory .github/scripts
* decrypt_secret.sh: the script to decrypt snowflake.travis.json.gpg
* run-tests-github.sh: the script to run spark connector IT test.

Files in ClusterTest 
* build.sbt: sbt build file for scala test
* run_cluster_test.sh: the script to run test
* src/main/python/*.py: the python test cases.
* src/main/scala//net/snowflake/spark/snowflake/ClusterTestCheckResult.scala: The class to check test result
* src/main/scala//net/snowflake/spark/snowflake/ClusterTest.scala: The driver class to run test
* src/main/scala//net/snowflake/spark/snowflake/testsuite/ClusterTestSuiteBase.scala: Test case base class.
* Files in src/main/scala: the Scala test related files.
