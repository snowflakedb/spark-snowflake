/*
 * Regression tests for internal finding 7bbc1438 (CWE-89): unsafe identifier quoting.
 * Co-authored with CoCo
 *
 * These tests assert the SECURE behavior of Utils.quotedName / quotedNameIgnoreCase /
 * isQuoted / ensureQuoted. They FAIL on the vulnerable code (embedded quotes not doubled,
 * isQuoted bypass) and PASS after the fix.
 *
 * NOTE ON CONVENTIONS: assumes ScalaTest AnyFunSuite. If this repo uses the older
 * org.scalatest.FunSuite, change the import + extends accordingly.
 */
package net.snowflake.spark.snowflake

import org.scalatest.funsuite.AnyFunSuite

class UtilsQuotingSecuritySuite extends AnyFunSuite {

  // ── The core injection payload from the finding ──────────────────────────────
  // A DataFrame column name crafted to break out of CREATE TABLE's column list.
  private val injection = """A" INT) AS SELECT CURRENT_USER()--"""

  test("quotedName doubles embedded double-quotes (no break-out)") {
    val out = Utils.quotedName(injection)
    // Must be wrapped in outer quotes...
    assert(out.startsWith("\"") && out.endsWith("\""))
    // ...and every interior quote must be doubled, so stripping the outer pair and
    // collapsing "" leaves NO lone quote.
    val inner = out.substring(1, out.length - 1)
    assert(!inner.replace("\"\"", "").contains("\""),
      s"quotedName left an unescaped double-quote: $out")
  }

  test("quotedNameIgnoreCase doubles embedded double-quotes") {
    val out = Utils.quotedNameIgnoreCase(injection)
    assert(out.startsWith("\"") && out.endsWith("\""))
    val inner = out.substring(1, out.length - 1)
    assert(!inner.replace("\"\"", "").contains("\""),
      s"quotedNameIgnoreCase left an unescaped double-quote: $out")
  }

  test("ensureQuoted neutralizes a payload that merely starts and ends with a quote") {
    // This is the isQuoted-bypass payload: starts and ends with ", so the OLD isQuoted
    // returned it unchanged. After the fix it must be treated as unquoted and escaped.
    val out = Utils.ensureQuoted(injection)
    val inner = out.substring(1, out.length - 1)
    assert(!inner.replace("\"\"", "").contains("\""),
      s"ensureQuoted passed through an injection payload unescaped: $out")
  }

  test("isQuoted rejects a malformed quoted string (interior lone quote)") {
    assert(!Utils.isQuoted(injection),
      "isQuoted must not treat a payload with an unescaped interior quote as quoted")
  }

  // ── Behavior preserved for legitimate identifiers ────────────────────────────
  test("isQuoted still accepts a simple well-formed quoted identifier") {
    assert(Utils.isQuoted("\"MyCol\""))
  }

  test("isQuoted still accepts a properly-escaped quoted identifier") {
    // "My""Col" is the canonical Snowflake escaping of My"Col
    assert(Utils.isQuoted("\"My\"\"Col\""))
  }

  test("ensureQuoted leaves a legitimate quoted identifier unchanged") {
    assert(Utils.ensureQuoted("\"MyCol\"") == "\"MyCol\"")
  }

  test("quotedName uppercases and wraps a legal unquoted identifier") {
    // Legal Spark identifier -> uppercased + quoted (existing behavior, must not regress)
    assert(Utils.quotedName("my_col") == "\"MY_COL\"")
  }

  test("round-trip: a name with a single embedded quote is recoverable") {
    val raw = "weird\"name"
    val quoted = Utils.quotedName(raw)
    // Snowflake unescaping: strip outer quotes, collapse "" back to "
    val recovered = quoted.substring(1, quoted.length - 1).replace("\"\"", "\"")
    assert(recovered == raw, s"round-trip failed: $raw -> $quoted -> $recovered")
  }
}
