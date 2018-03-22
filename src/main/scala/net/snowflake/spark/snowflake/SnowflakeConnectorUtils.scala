package net.snowflake.spark.snowflake

import java.nio.file.Paths
import java.security.InvalidKeyException

import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/** Connector utils, including what needs to be invoked to enable pushdowns. */
object SnowflakeConnectorUtils {

  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)


  /**
    * Check Spark version, if Spark version matches SUPPORT_SPARK_VERSION enable PushDown, otherwise disable it.
    */
  private val SUPPORT_SPARK_VERSION = "2.3"

  def checkVersionAndEnablePushdown(session: SparkSession): Boolean =
    if(session.version.startsWith(SUPPORT_SPARK_VERSION)){
      enablePushdownSession(session)
      true
    }else{
      disablePushdownSession(session)
      false
    }


  /** Enable more advanced query pushdowns to Snowflake.
    *
    * @param session The SparkSession for which pushdowns are to be enabled.
    */
  def enablePushdownSession(session: SparkSession): Unit = {
    if (!session.experimental.extraStrategies.exists(s =>
          s.isInstanceOf[SnowflakeStrategy]))
      session.experimental.extraStrategies ++= Seq(new SnowflakeStrategy)
  }

  /** Disable more advanced query pushdowns to Snowflake.
    *
    * @param session The SparkSession for which pushdowns are to be disabled.
    */
  def disablePushdownSession(session: SparkSession): Unit = {
    session.experimental.extraStrategies =
      session.experimental.extraStrategies.filterNot(strategy =>
        strategy.isInstanceOf[SnowflakeStrategy])
  }

  def setPushdownSession(session: SparkSession, enabled: Boolean): Unit = {
    if (enabled)
      enablePushdownSession(session)
    else
      disablePushdownSession(session)
  }

  // TODO: Improve error handling with retries, etc.

  @throws[SnowflakeConnectorException]
  def handleS3Exception(ex: Exception): Unit = {
    if (ex.getCause.isInstanceOf[InvalidKeyException]) {
      // Most likely cause: Unlimited strength policy files not installed
      var msg: String = "Strong encryption with Java JRE requires JCE " +
          "Unlimited Strength Jurisdiction Policy " +
          "files. " +
          "Follow JDBC client installation instructions " +
          "provided by Snowflake or contact Snowflake " +
          "Support. This needs to be installed in the Java runtime for all Spark executor nodes."

      log.error("JCE Unlimited Strength policy files missing: {}. {}.",
                ex.getMessage: Any,
                ex.getCause.getMessage: Any)

      val bootLib: String =
        java.lang.System.getProperty("sun.boot.library.path")

      if (bootLib != null) {
        msg += " The target directory on your system is: " + Paths
          .get(bootLib, "security")
          .toString
        log.error(msg)
      }

      throw new SnowflakeConnectorException(msg)
    } else
      throw ex
  }
}

class SnowflakeConnectorException(message: String) extends Exception(message)
class SnowflakePushdownException(message: String)
    extends SnowflakeConnectorException(message)
