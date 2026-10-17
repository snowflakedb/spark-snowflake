/*
 * Regression test for snowflakedb/spark-snowflake#673:
 * getCipherAndMetadata must not require SecureRandom.getInstance("SHA1PRNG", "SUN").
 *
 * Stock OpenJDK keeps NativePRNG / SHA1PRNG on the SUN provider, so removing SUN
 * without a replacement makes even `new SecureRandom()` fail (InternalError:
 * SHA-1 not available). FIPS JVMs still have some other SecureRandom SPI.
 * This test removes SUN and installs a tiny fallback provider so the JVM can
 * pick a default RNG the same way a FIPS configuration would.
 */
package net.snowflake.spark.snowflake.io

import java.security.{Provider, SecureRandomSpi, Security}
import java.util.Base64

import org.scalatest.funsuite.AnyFunSuite

class GetCipherAndMetadataSecureRandomSuite extends AnyFunSuite {

  // 128-bit AES master key, base64-encoded the same way a Snowflake query
  // stage master key is passed into getCipherAndMetadata.
  private val masterKey: String =
    Base64.getEncoder.encodeToString(Array.fill[Byte](16)(0x11.toByte))

  test("getCipherAndMetadata creates key material when SUN provider is absent") {
    val sun: Provider = Security.getProvider("SUN")
    assert(sun != null, "SUN provider must exist before the test can remove it")
    val fallback = new FallbackSecureRandomProvider
    Security.removeProvider("SUN")
    Security.insertProviderAt(fallback, 1)
    try {
      assert(
        Security.getProvider("SUN") == null,
        "SUN provider should be removed for this test"
      )
      val (cipher, matDesc, encKek, iv) =
        CloudStorageOperations.getCipherAndMetadata(masterKey, "query-id-673", "123")
      assert(cipher != null)
      assert(matDesc != null && matDesc.nonEmpty)
      assert(encKek != null && encKek.nonEmpty)
      assert(iv != null && iv.nonEmpty)
    } finally {
      Security.removeProvider(fallback.getName)
      if (Security.getProvider("SUN") == null && sun != null) {
        Security.insertProviderAt(sun, 1)
      }
    }
  }
}

/**
 * Test-only provider that advertises a SecureRandom algorithm so
 * `new SecureRandom()` can succeed after SUN is removed.
 */
class FallbackSecureRandomProvider
    extends Provider("TestFallbackRNG", "1.0", "test-only SecureRandom") {
  put(
    "SecureRandom.TestPRNG",
    classOf[FallbackSecureRandomSpi].getName
  )
}

class FallbackSecureRandomSpi extends SecureRandomSpi {
  private val delegate = new java.util.Random(42L)

  override def engineSetSeed(seed: Array[Byte]): Unit = {
    if (seed != null) {
      delegate.setSeed(java.util.Arrays.hashCode(seed).toLong)
    }
  }

  override def engineNextBytes(bytes: Array[Byte]): Unit = {
    delegate.nextBytes(bytes)
  }

  override def engineGenerateSeed(numBytes: Int): Array[Byte] = {
    val seed = new Array[Byte](numBytes)
    delegate.nextBytes(seed)
    seed
  }
}
