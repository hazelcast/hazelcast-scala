package com.hazelcast.Scala

import com.hazelcast.core.{ ITopic, Message, MessageListener }
import com.hazelcast.topic.ReliableMessageListener
import com.hazelcast.topic.impl.reliable.ReliableTopicService

final class HzTopic[T](private val topic: ITopic[T]) extends AnyVal {
  def isReliable: Boolean = topic.getServiceName == ReliableTopicService.SERVICE_NAME
  def onMessage(listener: Message[T] => Unit): ListenerRegistration = {
    val regId = topic addMessageListener new MessageListener[T] {
      def onMessage(msg: Message[T]) = listener(msg)
    }
    new ListenerRegistration {
      def cancel(): Unit = topic removeMessageListener regId
    }
  }
  def onMessage(startFrom: Long = -1, gapTolerant: Boolean = false)(listener: (Long, Message[T]) => Unit): ListenerRegistration = {
    require(isReliable, s"Must be reliable topic implementation: ${topic.getName}")
    val regId = topic addMessageListener new ReliableMessageListener[T] {
      private[this] var seq: Long = _
      def storeSequence(seq: Long): Unit = this.seq = seq
      def isLossTolerant = gapTolerant
      def isTerminal(t: Throwable) = true
      def retrieveInitialSequence(): Long = startFrom
      def onMessage(msg: Message[T]) = listener(seq, msg)
    }
    new ListenerRegistration {
      def cancel(): Unit = topic removeMessageListener regId
    }
  }

}
