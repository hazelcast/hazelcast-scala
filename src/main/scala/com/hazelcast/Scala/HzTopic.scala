package com.hazelcast.Scala

import com.hazelcast.cluster.Member
import com.hazelcast.topic.{ITopic, Message, MessageListener, ReliableMessageListener}
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
  def onSeqMessage(startFrom: Long = -1, gapTolerant: Boolean = false, runOn: ExecutionContext = null)(listener: PartialFunction[SeqMessage[T], Unit]): ListenerRegistration = {
    require(isReliable, s"Must be reliable topic implementation: ${topic.getName}")
    val regId = topic addMessageListener new PfProxy(listener, Option(runOn)) with ReliableMessageListener[T] {
      private[this] var seq: Long = _
      def storeSequence(seq: Long): Unit = this.seq = seq
      def isLossTolerant = gapTolerant
      def isTerminal(t: Throwable) = true
      def retrieveInitialSequence(): Long = startFrom
      def onMessage(msg: Message[T]) = invokeWith(new SeqMessage(seq, msg))
    }
    new ListenerRegistration {
      def cancel(): Boolean = topic removeMessageListener regId
    }
  }

}

final case class SeqMessage[T](seq: Long, value: T)(topicName: String, publishTime: Long, publishingMember: Member)
  extends Message[T](topicName, value, publishTime, publishingMember) {
  def this(seq: Long, msg: Message[T]) = this(seq, msg.getMessageObject)(String valueOf msg.getSource, msg.getPublishTime, msg.getPublishingMember)
}
