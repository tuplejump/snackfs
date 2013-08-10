package tj.util

import org.apache.thrift.async.AsyncMethodCallback

import scala.concurrent.{Future, promise}

object AsyncUtil {
  /**
   * A method that takes in a (partially applied) method which takes AsyncMethodCallback
   * and invokes it on completion or failure.
   *
   * @param f
   * @tparam T
   * @return
   */
  def executeAsync[T](f: AsyncMethodCallback[T] => Unit): Future[T] = {

    class PromisingHandler extends AsyncMethodCallback[T] {
      val p = promise[T]()

      def onComplete(p1: T) {
        p success p1
      }

      def onError(p1: Exception) {
        p failure p1
      }
    }

    val promisingHandler: PromisingHandler = new PromisingHandler()

    f(promisingHandler)

    promisingHandler.p.future
  }
}
