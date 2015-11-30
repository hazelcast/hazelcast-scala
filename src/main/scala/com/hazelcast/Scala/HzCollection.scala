package com.hazelcast.Scala

import com.hazelcast.core.ICollection
import com.hazelcast.core.ItemEventType
import com.hazelcast.core.Member
import com.hazelcast.core.ItemListener
import com.hazelcast.core.ItemEvent

class HzCollection[T](private val coll: ICollection[T]) extends AnyVal {
  def onChange(pf: PartialFunction[(ItemEventType, Member), Unit]): ListenerRegistration = {
    val listener = new ItemListener[T] {
      def itemAdded(evt: ItemEvent[T]) = hear(evt)
      def itemRemoved(evt: ItemEvent[T]) = hear(evt)
      @inline def hear(evt: ItemEvent[T]): Unit = {
        val tuple = (evt.getEventType, evt.getMember)
        if (pf.isDefinedAt(tuple)) pf(tuple)
      }
    }
    val regId = coll.addItemListener(listener, false)
    new ListenerRegistration {
      def cancel() = coll.removeItemListener(regId)
    }
  }
  def onItem(pf: PartialFunction[(ItemEventType, Member, T), Unit]): ListenerRegistration = {
    val listener = new ItemListener[T] {
      def itemAdded(evt: ItemEvent[T]) = hear(evt)
      def itemRemoved(evt: ItemEvent[T]) = hear(evt)
      @inline def hear(evt: ItemEvent[T]): Unit = {
        val tuple = (evt.getEventType, evt.getMember, evt.getItem)
        if (pf.isDefinedAt(tuple)) pf(tuple)
      }
    }
    val regId = coll.addItemListener(listener, true)
    new ListenerRegistration {
      def cancel() = coll.removeItemListener(regId)
    }
  }
}
