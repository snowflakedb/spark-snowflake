/*
 * Regression tests for internal finding 5cda28bb (CWE-918): OAuth token URL SSRF.
 * Co-authored with CoCo
 *
 * Asserts validateOAuthTokenRequestUrl rejects non-HTTPS, loopback, and link-local/IMDS
 * targets, and accepts a normal HTTPS endpoint.
 *
 * REQUIRES: validateOAuthTokenRequestUrl must be reachable from this package. The fix
 * defines it `private def`; add-tests.sh changes it to `private[snowflake] def` so this
 * test (same top-level package) can call it. If ServerConnection exposes it on the
 * companion object, call it as ServerConnection.validateOAuthTokenRequestUrl(...).
 *
 * NOTE: assumes ScalaTest AnyFunSuite; switch to org.scalatest.FunSuite if needed.
 */
package net.snowflake.spark.snowflake

import org.scalatest.funsuite.AnyFunSuite

class OAuthUrlValidationSecuritySuite extends AnyFunSuite {

  private def validate(url: String): Unit =
    ServerConnection.validateOAuthTokenRequestUrl(url)

  test("rejects non-HTTPS scheme") {
    assertThrows[IllegalArgumentException](validate("http://idp.example.com/token"))
  }

  test("rejects loopback host") {
    assertThrows[IllegalArgumentException](validate("https://localhost/token"))
    assertThrows[IllegalArgumentException](validate("https://127.0.0.1/token"))
  }

  test("rejects link-local / cloud IMDS") {
    assertThrows[IllegalArgumentException](validate("https://169.254.169.254/latest/meta-data"))
  }

  test("rejects a malformed URI") {
    assertThrows[IllegalArgumentException](validate("not a url"))
  }

  test("accepts a normal external HTTPS endpoint") {
    // Should NOT throw.
    validate("https://myaccount.snowflakecomputing.com/oauth/token-request")
    validate("https://idp.example.com/oauth2/token")
  }
}
