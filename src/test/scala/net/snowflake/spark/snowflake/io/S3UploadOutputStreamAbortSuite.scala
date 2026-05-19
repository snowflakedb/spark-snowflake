/*
 * Copyright 2015-2026 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.snowflake.spark.snowflake.io

import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AbstractAmazonS3
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.{
  AbortMultipartUploadRequest,
  CompleteMultipartUploadRequest,
  CompleteMultipartUploadResult,
  InitiateMultipartUploadRequest,
  InitiateMultipartUploadResult,
  ObjectMetadata,
  PutObjectResult,
  UploadPartRequest,
  UploadPartResult
}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

/**
 * Unit tests for `S3UploadOutputStream#abort()`, the cleanup path used when
 * a Spark task is interrupted (e.g. speculative-task kill) and we must
 * abandon the partial upload instead of committing it.
 *
 * Background: under Spark speculation a killed task could overwrite a
 * successful task's staged file because `close()` was called unconditionally
 * in the `finally` block of `doUploadPartition`, committing a partial
 * multipart upload (or single-shot putObject) over the good object. The fix
 * skips `close()` and calls `abort()` instead so the partial upload is
 * abandoned.
 *
 * The invariants exercised here are:
 *   1. abort() must NEVER call any S3 API that commits data, regardless
 *      of whether multipart was initiated.
 *   2. If multipart WAS initiated, abort() must explicitly issue
 *      AbortMultipartUpload so already-uploaded parts don't leak in S3.
 *   3. abort() must swallow cleanup failures (it runs in a finally
 *      block).
 */
class S3UploadOutputStreamAbortSuite extends AnyFunSuite {

  private val storageInfo = Map(
    StorageInfo.BUCKET_NAME -> "test_bucket",
    StorageInfo.PREFIX -> "test_prefix/"
  )
  private val fileName = "35.CSV.gz"

  /**
   * Records every call we care about. Inherits from `AbstractAmazonS3`,
   * which throws `UnsupportedOperationException` for every method, so any
   * unexpected use of the S3 client surfaces as a test failure rather than
   * silently no-opping.
   */
  private class RecordingS3 extends AbstractAmazonS3 {
    val initiate = mutable.ArrayBuffer[InitiateMultipartUploadRequest]()
    val uploadPart = mutable.ArrayBuffer[UploadPartRequest]()
    val complete = mutable.ArrayBuffer[CompleteMultipartUploadRequest]()
    val abort = mutable.ArrayBuffer[AbortMultipartUploadRequest]()
    val putObjectKeys = mutable.ArrayBuffer[(String, String)]()
    var uploadId: String = "test-upload-id"
    var abortShouldThrow: Throwable = null

    override def initiateMultipartUpload(
      req: InitiateMultipartUploadRequest
    ): InitiateMultipartUploadResult = synchronized {
      initiate += req
      val r = new InitiateMultipartUploadResult()
      r.setUploadId(uploadId)
      r
    }

    override def uploadPart(
      req: UploadPartRequest
    ): UploadPartResult = synchronized {
      uploadPart += req
      val r = new UploadPartResult()
      r.setETag(s"etag-${uploadPart.size}")
      r.setPartNumber(req.getPartNumber)
      r
    }

    override def completeMultipartUpload(
      req: CompleteMultipartUploadRequest
    ): CompleteMultipartUploadResult = synchronized {
      complete += req
      new CompleteMultipartUploadResult()
    }

    override def abortMultipartUpload(
      req: AbortMultipartUploadRequest
    ): Unit = synchronized {
      abort += req
      if (abortShouldThrow != null) throw abortShouldThrow
    }

    override def putObject(
      bucket: String,
      key: String,
      input: java.io.InputStream,
      meta: ObjectMetadata
    ): PutObjectResult = synchronized {
      putObjectKeys += ((bucket, key))
      new PutObjectResult()
    }
  }

  private def assertNoCommits(s3: RecordingS3): Unit = {
    assert(s3.putObjectKeys.isEmpty, "putObject must not be called")
    assert(s3.complete.isEmpty, "completeMultipartUpload must not be called")
  }

  test("abort() commits nothing when no data was written") {
    val s3 = new RecordingS3
    val stream = new S3UploadOutputStream(
      s3, new ObjectMetadata(), storageInfo, 1024, fileName)

    stream.abort()

    assertNoCommits(s3)
    assert(s3.initiate.isEmpty,
      "no multipart upload should have been initiated")
    assert(s3.abort.isEmpty,
      "no multipart upload to abort if none was initiated")
  }

  test("abort() commits nothing when data fits in single chunk " +
    "(multipart never initiated)") {
    val s3 = new RecordingS3
    val bufferSize = 1024
    val stream = new S3UploadOutputStream(
      s3, new ObjectMetadata(), storageInfo, bufferSize, fileName)

    // Strictly less than bufferSize: this is the single-shot putObject
    // path. close() would have called putObject; abort() must not.
    for (i <- 1 to 100) stream.write(i)

    stream.abort()

    assertNoCommits(s3)
    assert(s3.initiate.isEmpty,
      "no multipart upload should have been initiated")
    assert(s3.abort.isEmpty,
      "no multipart upload to abort if none was initiated")
  }

  test("abort() aborts the in-flight multipart upload after parts uploaded") {
    val s3 = new RecordingS3
    s3.uploadId = "test-upload-id-abc123"
    val bufferSize = 64
    val stream = new S3UploadOutputStream(
      s3, new ObjectMetadata(), storageInfo, bufferSize, fileName)

    // Write bufferSize+1 bytes via write(int) so the (bufferSize+1)th
    // call triggers uploadDataChunk(false) -> doUploadOnePart(), which
    // initiates the multipart upload and uploads part 1.
    for (i <- 1 to bufferSize + 1) stream.write(i)

    // Sanity: multipart was initiated and one part was uploaded.
    assert(s3.initiate.size == 1, "multipart upload should be initiated once")
    assert(s3.uploadPart.size == 1, "exactly one part should be uploaded")

    stream.abort()

    // The multipart upload must be explicitly aborted exactly once,
    // with the correct (bucket, key, uploadId).
    assert(s3.abort.size == 1, "abortMultipartUpload should be called once")
    val req = s3.abort.head
    assert(req.getUploadId == s3.uploadId,
      s"unexpected uploadId: ${req.getUploadId}")
    assert(req.getBucketName == "test_bucket",
      s"unexpected bucket: ${req.getBucketName}")
    assert(req.getKey == "test_prefix/" + fileName,
      s"unexpected key: ${req.getKey}")

    // And it must NOT be committed - this is the regression guard.
    assertNoCommits(s3)
  }

  test("abort() swallows S3 abort failures (runs in a finally block)") {
    val s3 = new RecordingS3
    s3.abortShouldThrow = new RuntimeException("simulated S3 abort failure")
    val bufferSize = 64
    val stream = new S3UploadOutputStream(
      s3, new ObjectMetadata(), storageInfo, bufferSize, "f.gz")

    for (i <- 1 to bufferSize + 1) stream.write(i)

    // Must not propagate. If this throws, the original cause of the
    // abort (e.g. speculative-task kill) would be masked by a cleanup
    // failure, which is exactly what the production try/finally is
    // trying to avoid.
    stream.abort()

    // We did still attempt the abort.
    assert(s3.abort.size == 1, "abort attempt should still have been made")
  }
}
