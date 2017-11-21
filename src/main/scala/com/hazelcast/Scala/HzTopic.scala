package com.hazelcast.Scala

import com.hazelcast.core.{ ITopic, Message, MessageListener }
import com.hazelcast.topic.ReliableMessageListener
import com.hazelcast.topic.impl.reliable.ReliableTopicService
import scala.concurrent.ExecutionContext

final class HzTopic[T](private val topic: ITopic[T]) extends AnyVal {
  def isReliable: Boolean = topic.getServiceName == ReliableTopicService.SERVICE_NAME
  def onMessage(runOn: ExecutionContext = null)(listener: PartialFunction[Message[T], Unit]): ListenerRegistration = {
    val regId = topic addMessageListener new PfProxy(listener, Option(runOn)) with MessageListener[T] {
      def onMessage(msg: Message[T]) = invokeWith(msg)
    }
    new ListenerRegistration {
      def cancel(): Boolean = topic removeMessageListener regId
    }
  }
  def onSeqMessage(startFrom: Long = -1, gapTolerant: Boolean = false, runOn: ExecutionContext = null)(listener: PartialFunction[(Long, Message[T]), Unit]): ListenerRegistration = {
    require(isReliable, s"Must be reliable topic implementation: ${topic.getName}")
    val regId = topic addMessageListener new PfProxy(listener, Option(runOn)) with ReliableMessageListener[T] {
      private[this] var seq: Long = _
      def storeSequence(seq: Long): Unit = this.seq = seq
      def isLossTolerant = gapTolerant
      def isTerminal(t: Throwable) = true
      def retrieveInitialSequence(): Long = startFrom
      def onMessage(msg: Message[T]) = invokeWith(seq -> msg)
    }
    new ListenerRegistration {
      def cancel(): Boolean = topic removeMessageListener regId
    }
  }

}
