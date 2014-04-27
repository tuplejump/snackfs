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
          log.error(x, x.getMessage)
          throw x
        }
        else
          handleFailure(fn, params, retryCount)
      case y: Exception =>
        log.error(y, y.getMessage)
        throw y
    }
    mayBeResult
  }
}
