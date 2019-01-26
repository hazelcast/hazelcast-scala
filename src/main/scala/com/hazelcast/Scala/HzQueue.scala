package com.hazelcast.Scala

import com.hazelcast.core.BaseQueue
import scala.concurrent.duration.FiniteDuration
import com.hazelcast.core.TransactionalQueue

final class HzQueue[T](private val queue: BaseQueue[T]) extends AnyVal {
  def poll(timeout: FiniteDuration): T = queue.poll(timeout.length, timeout.unit)
  def offer(entry: T, timeout: FiniteDuration): Boolean = queue.offer(entry, timeout.length, timeout.unit)
}
final class HzTxQueue[T](private val queue: TransactionalQueue[T]) extends AnyVal {
  def peek(timeout: FiniteDuration): T = queue.peek(timeout.length, timeout.unit)
}
