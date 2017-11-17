package com.hazelcast.Scala

import com.hazelcast.core.ExecutionCallback
import scala.concurrent.Promise
import java.util.concurrent.ExecutionException
import scala.util.control.NonFatal

private[Scala] final class FutureCallback[X, R](nullReplacement: R = null.asInstanceOf[R])(implicit map: X => R)
    extends ExecutionCallback[X] {

  private[this] val promise = Promise[R]
  def future = promise.future

  def onFailure(th: Throwable) = th match {
    case e: ExecutionException => onFailure(e.getCause)
    case e => promise.failure(e)
  }
  def onResponse(res: X) = res match {
    case th: Throwable => onFailure(th)
    case null => promise success nullReplacement
    case value =>
      try {
        promise success map(value)
      } catch {
        case NonFatal(e) => onFailure(e)
      }
  }
}
