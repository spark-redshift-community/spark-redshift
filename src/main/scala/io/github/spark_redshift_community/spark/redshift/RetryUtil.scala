package com.databricks.spark.redshift

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object RetryUtil {
  private val log = LoggerFactory.getLogger(getClass)

  // Number of times to retry the function after a failure
  val DEFAULT_NUM_OF_RETRIES = 5

  // This value is in milliseconds
  val DEFAULT_RETRY_INTERVAL = 1000

  /**
   * Execute a given function and then determine if the function succeeded or failed.
   * If the function succeeded then return the value from the function.
   * If the function failed then attempt to retry based on the number of retries provided.
   * An included wait time between retries is based on the provided retryInterval value (ms).
   */
  @annotation.tailrec
  final def retry[T] (numOfRetries: Int = DEFAULT_NUM_OF_RETRIES)
    (fn: => T,
     retryInterval: Int = DEFAULT_RETRY_INTERVAL,
     retryMessage: String,
     failureMessage: String
    ): T = {
    Try {
      fn
    } match {
      case Success(x) => x
      case Failure(e) =>
        if (numOfRetries <= 0) {
          log.error(failureMessage, e)
          throw e
        } else {
          log.warn(retryMessage, e)
          Thread.sleep(retryInterval)
          retry(numOfRetries - 1)(fn, retryInterval, retryMessage, failureMessage)
        }
    }
  }
}
