package com.hazelcast.Scala

import com.hazelcast._
import scala.concurrent.ExecutionContext

private[Scala] class MapListener(pf: PartialFunction[MapEvent, Unit], ec: Option[ExecutionContext]) extends PfProxy(pf, ec)
    with map.listener.MapListener
    with map.listener.MapClearedListener
    with map.listener.MapEvictedListener {
  def mapCleared(evt: core.MapEvent): Unit = invokeWith(MapCleared(evt.getNumberOfEntriesAffected)(evt))
  def mapEvicted(evt: core.MapEvent): Unit = invokeWith(MapEvicted(evt.getNumberOfEntriesAffected)(evt))
}
private[Scala] trait EntryAddedListener[K, V] extends map.listener.EntryAddedListener[K, V] { self: PfProxy[EntryEvent[K, V]] =>
  def entryAdded(evt: core.EntryEvent[K, V]): Unit = invokeWith(EntryAdded(evt.getKey, evt.getValue)(evt))
}
private[Scala] trait EntryEvictedListener[K, V] extends map.listener.EntryEvictedListener[K, V] { self: PfProxy[EntryEvent[K, V]] =>
  def entryEvicted(evt: core.EntryEvent[K, V]): Unit = invokeWith(EntryEvicted(evt.getKey, evt.getValue)(evt))
}
private[Scala] trait EntryMergedListener[K, V] extends map.listener.EntryMergedListener[K, V] { self: PfProxy[EntryEvent[K, V]] =>
  def entryMerged(evt: core.EntryEvent[K, V]): Unit = invokeWith(EntryMerged(evt.getKey, Option(evt.getOldValue), evt.getMergingValue, Option(evt.getValue))(evt))
}
private[Scala] trait EntryRemovedListener[K, V] extends map.listener.EntryRemovedListener[K, V] { self: PfProxy[EntryEvent[K, V]] =>
  def entryRemoved(evt: core.EntryEvent[K, V]): Unit = invokeWith(EntryRemoved(evt.getKey, evt.getOldValue)(evt))
}
private[Scala] trait EntryUpdatedListener[K, V] extends map.listener.EntryUpdatedListener[K, V] { self: PfProxy[EntryEvent[K, V]] =>
  def entryUpdated(evt: core.EntryEvent[K, V]): Unit = invokeWith(EntryUpdated(evt.getKey, evt.getOldValue, evt.getValue)(evt))
}

private[Scala] class EntryListener[K, V](pf: PartialFunction[EntryEvent[K, V], Unit], ec: Option[ExecutionContext]) extends PfProxy(pf, ec)
  with map.listener.MapListener
  with EntryAddedListener[K, V]
  with EntryEvictedListener[K, V]
  with EntryMergedListener[K, V]
  with EntryRemovedListener[K, V]
  with EntryUpdatedListener[K, V]

private[Scala] trait KeyAddedListener[K] extends map.listener.EntryAddedListener[K, Object] { self: PfProxy[KeyEvent[K]] =>
  def entryAdded(evt: core.EntryEvent[K, Object]): Unit = invokeWith(KeyAdded(evt.getKey)(evt))
}
private[Scala] trait KeyEvictedListener[K] extends map.listener.EntryEvictedListener[K, Object] { self: PfProxy[KeyEvent[K]] =>
  def entryEvicted(evt: core.EntryEvent[K, Object]): Unit = invokeWith(KeyEvicted(evt.getKey)(evt))
}
private[Scala] trait KeyMergedListener[K] extends map.listener.EntryMergedListener[K, Object] { self: PfProxy[KeyEvent[K]] =>
  def entryMerged(evt: core.EntryEvent[K, Object]): Unit = invokeWith(KeyMerged(evt.getKey)(evt))
}
private[Scala] trait KeyRemovedListener[K] extends map.listener.EntryRemovedListener[K, Object] { self: PfProxy[KeyEvent[K]] =>
  def entryRemoved(evt: core.EntryEvent[K, Object]): Unit = invokeWith(KeyRemoved(evt.getKey)(evt))
}
private[Scala] trait KeyUpdatedListener[K] extends map.listener.EntryUpdatedListener[K, Object] { self: PfProxy[KeyEvent[K]] =>
  def entryUpdated(evt: core.EntryEvent[K, Object]): Unit = invokeWith(KeyUpdated(evt.getKey)(evt))
}

private[Scala] class KeyListener[K](pf: PartialFunction[KeyEvent[K], Unit], ec: Option[ExecutionContext]) extends PfProxy(pf, ec)
  with map.listener.MapListener
  with KeyAddedListener[K]
  with KeyEvictedListener[K]
  with KeyMergedListener[K]
  with KeyRemovedListener[K]
  with KeyUpdatedListener[K]

sealed abstract class MapEvent(evt: core.MapEvent) {
  def member = evt.getMember
  override def toString() = evt.toString()
}
final case class MapCleared(entriesCleared: Int)(evt: core.MapEvent) extends MapEvent(evt)
final case class MapEvicted(entriesEvicted: Int)(evt: core.MapEvent) extends MapEvent(evt)

sealed abstract class KeyEvent[K](evt: core.EntryEvent[K, _]) {
  def member = evt.getMember
  def key: K
  override def toString() = evt.toString()
}
final case class KeyAdded[K](key: K)(evt: core.EntryEvent[K, Object]) extends KeyEvent(evt)
final case class KeyEvicted[K](key: K)(evt: core.EntryEvent[K, Object]) extends KeyEvent(evt)
final case class KeyRemoved[K](key: K)(evt: core.EntryEvent[K, Object]) extends KeyEvent(evt)
final case class KeyMerged[K](key: K)(evt: core.EntryEvent[K, Object]) extends KeyEvent(evt)
final case class KeyUpdated[K](key: K)(evt: core.EntryEvent[K, Object]) extends KeyEvent(evt)

sealed abstract class EntryEvent[K, V](evt: core.EntryEvent[K, V]) extends KeyEvent(evt)
final case class EntryAdded[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryEvicted[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryRemoved[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryMerged[K, V](key: K, oldValue: Option[V], mergeValue: V, newValue: Option[V])(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryUpdated[K, V](key: K, oldValue: V, newValue: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)

final case class PartitionLossEvent(member: core.Member, partitionId: Int)(evt: map.MapPartitionLostEvent) {
  override def toString() = evt.toString()
}
