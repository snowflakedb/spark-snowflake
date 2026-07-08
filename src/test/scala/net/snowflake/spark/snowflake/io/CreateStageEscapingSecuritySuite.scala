/*
 * Regression tests for internal finding 80cb0337 (CWE-89): CREATE STAGE SQL injection.
 * Co-authored with CoCo
 *
 * Asserts the SQL-escaping helpers neutralize single-quote / double-quote injection in
 * credential values, bucket/prefix, and the stage identifier.
 *
 * REQUIRES: escapeSqlStringLiteral / escapeSqlIdentifier are defined `private def` by the
 * fix; add-tests.sh changes them to `private[snowflake] def` so this test (sub-package of
 * net.snowflake.spark.snowflake) can call them on the CloudStorageOperations object.
 *
 * NOTE: assumes ScalaTest AnyFunSuite; switch to org.scalatest.FunSuite if needed.
 */
package net.snowflake.spark.snowflake.io

import org.scalatest.funsuite.AnyFunSuite

class CreateStageEscapingSecuritySuite extends AnyFunSuite {

  test("escapeSqlStringLiteral doubles single quotes (closes string-literal break-out)") {
    // Payload that would otherwise close the aws_secret_key='...' literal and inject SQL.
    val payload = "abc' ; DROP TABLE x --"
    val escaped = CloudStorageOperations.escapeSqlStringLiteral(payload)
    // After escaping, there must be no lone single quote (all doubled).
    assert(!escaped.replace("''", "").contains("'"),
      s"escapeSqlStringLiteral left a lone single quote: $escaped")
  }

  test("escapeSqlStringLiteral is a no-op for clean values") {
    assert(CloudStorageOperations.escapeSqlStringLiteral("AKIAEXAMPLE") == "AKIAEXAMPLE")
  }

  test("escapeSqlIdentifier doubles embedded double-quotes and wraps") {
    val payload = """s") ; DROP STAGE x --"""
    val escaped = CloudStorageOperations.escapeSqlIdentifier(payload)
    assert(escaped.startsWith("\"") && escaped.endsWith("\""))
    val inner = escaped.substring(1, escaped.length - 1)
    assert(!inner.replace("\"\"", "").contains("\""),
      s"escapeSqlIdentifier left an unescaped double-quote: $escaped")
  }

  test("escapeSqlIdentifier wraps a clean stage name") {
    assert(CloudStorageOperations.escapeSqlIdentifier("my_stage") == "\"my_stage\"")
  }
}
