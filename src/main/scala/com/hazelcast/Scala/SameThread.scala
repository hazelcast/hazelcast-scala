package com.hazelcast.Scala

import scala.concurrent.ExecutionContext

private[Scala] object SameThread extends ExecutionContext {
  def execute(r: Runnable) = r.run()
  def reportFailure(cause: Throwable) = throw cause
}
