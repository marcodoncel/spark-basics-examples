package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

object LogManager extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevelToWarn() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        "To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
  def setStreamingLogLevelToWarn(level: Level) {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to "+level+" for streaming example." +
        "To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(level)
    }
  }
  def isLoggerDebugEnabled() : Boolean = {
    Logger.getRootLogger.isDebugEnabled
  }

  def logErrorMessage(message: String) = {
    logError(message)
  }
  def logInfoMessage(message: String) = {
    logInfo(message)
  }
  def logWarnMessage(message: String) = {
    logWarning(message)
  }
  def logDebugMessage(message: String) = {
    logDebug(message)
  }
}