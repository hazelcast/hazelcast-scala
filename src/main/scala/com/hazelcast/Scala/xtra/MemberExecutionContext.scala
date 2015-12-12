package com.hazelcast.Scala.xtra

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import com.hazelcast.Scala.ToLocal
import com.hazelcast.core.IExecutorService
import com.hazelcast.logging.ILogger

/**
  * Execution context for local member.
  * NOTICE: Will not work on client!
  */
class MemberExecutionContext(exe: IExecutorService) extends ExecutionContext {
  private[this] val logger: ILogger = {
    exe.getClass.getDeclaredFields.collectFirst {
      case field if classOf[ILogger].isAssignableFrom(field.getType) =>
        field.setAccessible(true)
        field.get(exe).asInstanceOf[ILogger]
    } getOrElse {
      sys.error("Cannot find ILogger. Perhaps this is a client?")
    }
  }
  def execute(runnable: Runnable): Unit = {
    val wrapper = new Runnable {
      def run = try {
        runnable.run()
      } catch {
        case NonFatal(e) => reportFailure(e)
      }
    }
    exe.execute(runnable, ToLocal.selector)
  }
  def reportFailure(cause: Throwable): Unit = logger.severe(cause)

}
