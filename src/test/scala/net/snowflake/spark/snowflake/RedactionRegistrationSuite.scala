/*
 * Regression test for internal finding ddf7a7e2 (CWE-532): sensitive options must be
 * registered with Spark's option redaction so pem_private_key etc. don't leak to event
 * logs / History Server / plan strings.
 * Co-authored with CoCo
 *
 * REQUIRES: DefaultSource.ensureConnectorOptionsRedacted made `private[snowflake]`
 * (add-tests.sh handles this) and a no-arg DefaultSource constructor (Spark DataSource v1
 * providers normally have one). If DefaultSource has no no-arg ctor, change the
 * instantiation below to `new DefaultSource(DefaultJDBCWrapper)`.
 *
 * NOTE: assumes ScalaTest AnyFunSuite (confirmed repo convention).
 */
package net.snowflake.spark.snowflake

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class RedactionRegistrationSuite extends AnyFunSuite {

  private val REDACT_KEY = "spark.sql.redaction.options.regex"

  test("connector registers sensitive option names for Spark redaction") {
    val spark = SparkSession.builder()
      .appName("RedactionRegistrationSuite")
      .master("local")
      .getOrCreate()

    // Start from Spark's documented default so the assertion is deterministic.
    spark.conf.set(REDACT_KEY, "(?i)url")

    new DefaultSource().ensureConnectorOptionsRedacted(spark)

    val regex = spark.conf.get(REDACT_KEY).toLowerCase
    assert(regex.contains("pem_private_key"), s"pem_private_key not registered: $regex")
    assert(regex.contains("awssecretkey"),    s"awssecretkey not registered: $regex")
    assert(regex.contains("oauthclientsecret"), s"oauthclientsecret not registered: $regex")
    // The pre-existing pattern must be preserved, not clobbered.
    assert(regex.contains("url"), s"original redaction pattern lost: $regex")
  }

  test("ensureConnectorOptionsRedacted is idempotent (no unbounded growth)") {
    val spark = SparkSession.builder()
      .appName("RedactionRegistrationSuite2")
      .master("local")
      .getOrCreate()
    spark.conf.set(REDACT_KEY, "(?i)url")
    val ds = new DefaultSource()
    ds.ensureConnectorOptionsRedacted(spark)
    val once = spark.conf.get(REDACT_KEY)
    ds.ensureConnectorOptionsRedacted(spark)
    val twice = spark.conf.get(REDACT_KEY)
    assert(once == twice, "redaction regex should not be appended to on every call")
  }
}
