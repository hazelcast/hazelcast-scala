package com.hazelcast.Scala

import com.hazelcast.core.ICollection
import com.hazelcast.core.ItemEventType
import com.hazelcast.core.Member
import com.hazelcast.core.ItemListener
import com.hazelcast.core.ItemEvent
import scala.concurrent.ExecutionContext

class HzCollection[T](private val coll: ICollection[T]) extends AnyVal {
  /** Get notified on changes, but not the content of the change. */
  def onChange(runOn: ExecutionContext = null)(pf: PartialFunction[(ItemEventType, Member), Unit]): ListenerRegistration = {
    val listener = new PfProxy(pf, Option(runOn)) with ItemListener[T] {
      def itemAdded(evt: ItemEvent[T]) = invokeWith(evt.getEventType -> evt.getMember)
      def itemRemoved(evt: ItemEvent[T]) = invokeWith(evt.getEventType -> evt.getMember)
    }
    val regId = coll.addItemListener(listener, false)
    new ListenerRegistration {
      def cancel() = coll.removeItemListener(regId)
    }
  }
  /** Get notified on changes, including the value that was added/removed. */
  def onItem(runOn: ExecutionContext = null)(pf: PartialFunction[(ItemEventType, Member, T), Unit]): ListenerRegistration = {
    val listener = new PfProxy(pf, Option(runOn)) with ItemListener[T] {
      def itemAdded(evt: ItemEvent[T]) = invokeWith((evt.getEventType, evt.getMember, evt.getItem))
      def itemRemoved(evt: ItemEvent[T]) = invokeWith((evt.getEventType, evt.getMember, evt.getItem))
    }
    val regId = coll.addItemListener(listener, true)
    new ListenerRegistration {
      def cancel() = coll.removeItemListener(regId)
    }
  }
}
