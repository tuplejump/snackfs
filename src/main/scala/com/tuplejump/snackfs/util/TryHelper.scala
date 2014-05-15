package com.tuplejump.snackfs.util

import scala.util.Try
import org.apache.cassandra.thrift.{TimedOutException, UnavailableException}
import com.twitter.logging.Logger

object TryHelper {

  def handleFailure[T, R](fn: T => Try[R], params: T, retries: Int = 0): Try[R] = {
    val log = Logger.get(fn.getClass)
    val mayBeResult = fn(params).recoverWith {
      case x@(_: UnavailableException | _: TimedOutException) =>
        Thread.sleep(15)
        val retryCount = retries + 1
        if (retryCount >= 3) {
          log.error("Function: %s failed for parameters: %s", fn.toString, params.toString)
          log.error(x, "Failed after retrying for 3 times")
          throw x
        }
        else
          handleFailure(fn, params, retryCount)
      case y: Exception =>
        log.error("Function: %s failed for parameters: %s", fn.toString, params.toString)
        log.error(y, "Got an exception that could not be handled")
        throw y
    }
    mayBeResult
  }
}
