package net.snowflake.spark.snowflake.test

import net.snowflake.client.jdbc.{ErrorCode, SnowflakeSQLException}
import net.snowflake.spark.snowflake.test.TestHookFlag.TestHookFlag
import net.snowflake.spark.snowflake.LoggerWrapperFactory

object TestHookFlag extends Enumeration {
  type TestHookFlag = Value

  // All predefined test hook's name start with TH_ (TEST HOOK).
  val TH_WRITE_ERROR_AFTER_DROP_OLD_TABLE = Value("TH_WRITE_ERROR_AFTER_DROP_OLD_TABLE")
  val TH_WRITE_ERROR_AFTER_CREATE_NEW_TABLE = Value("TH_WRITE_ERROR_AFTER_CREATE_NEW_TABLE")
  val TH_WRITE_ERROR_AFTER_TRUNCATE_TABLE = Value("TH_WRITE_ERROR_AFTER_TRUNCATE_TABLE")
  val TH_WRITE_ERROR_AFTER_COPY_INTO = Value("TH_WRITE_ERROR_AFTER_COPY_INTO")
  val TH_GCS_UPLOAD_RAISE_EXCEPTION = Value("TH_GCS_UPLOAD_RAISE_EXCEPTION")
  val TH_COPY_INTO_TABLE_MISS_FILES_SUCCESS = Value("TH_COPY_INTO_TABLE_MISS_FILES_SUCCESS")
  val TH_COPY_INTO_TABLE_MISS_FILES_FAIL = Value("TH_COPY_INTO_TABLE_MISS_FILES_FAIL")
  val TH_ARROW_DRIVER_FAIL_CLOSE_RESULT_SET = Value("TH_ARROW_DRIVER_FAIL_CLOSE_RESULT_SET")
  val TH_ARROW_FAIL_OPEN_RESULT_SET = Value("TH_ARROW_FAIL_OPEN_RESULT_SET")
  val TH_ARROW_FAIL_READ_RESULT_SET = Value("TH_ARROW_FAIL_READ_RESULT_SET")
  val TH_FAIL_CREATE_DOWNLOAD_STREAM = Value("TH_FAIL_CREATE_DOWNLOAD_STREAM")
  val TH_FAIL_CREATE_UPLOAD_STREAM = Value("TH_FAIL_CREATE_UPLOAD_STREAM")
}

object TestHook {
  val log = LoggerWrapperFactory.getLoggerWrapper(getClass)

  private val ENABLED_TEST_FLAGS =
    new scala.collection.mutable.HashSet[TestHookFlag]()

  private var IS_TEST_ENABLED = false

  private val TEST_MESSAGE_PREFIX =
    "Internal test error (should NOT be seen by user):"

  // Enable test
  private[snowflake] def enableTestHook() : Unit = {
    IS_TEST_ENABLED = true
  }

  // Disable test
  private[snowflake] def disableTestHook() : Unit = {
    IS_TEST_ENABLED = false
    ENABLED_TEST_FLAGS.clear()
  }

  // Enable a specific test flag
  private[snowflake] def enableTestFlag(testFlag : TestHookFlag): Unit = {
    enableTestHook()
    if (!ENABLED_TEST_FLAGS.contains(testFlag)) {
      ENABLED_TEST_FLAGS.add(testFlag)
    }
  }

  // Enable a specific test flag only (all other flags are disabled)
  private[snowflake] def enableTestFlagOnly(testFlag : TestHookFlag): Unit = {
    disableTestHook()
    enableTestFlag(testFlag)
  }

  // Disable a specific test flag
  private[snowflake] def disableTestFlag(testFlag : TestHookFlag): Unit = {
    if (ENABLED_TEST_FLAGS.contains(testFlag)) {
      ENABLED_TEST_FLAGS.remove(testFlag)
    }
    if (ENABLED_TEST_FLAGS.isEmpty) {
      disableTestHook()
    }
  }

  // Check whether a flag is enabled
  private[snowflake] def isTestFlagEnabled(testFlag : TestHookFlag): Boolean = {
    IS_TEST_ENABLED && ENABLED_TEST_FLAGS.contains(testFlag)
  }

  // Raise exception if the specific test flag is enabled.
  private[snowflake] def raiseExceptionIfTestFlagEnabled(testFlag: TestHookFlag,
                                                         errorMessage: String)
  : Unit = {
    if (isTestFlagEnabled(testFlag)) {
      throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR,
        s"$TEST_MESSAGE_PREFIX  $errorMessage")
    }
  }
}
