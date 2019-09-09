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
private[Scala] trait EntryExpiredListener[K, V] extends map.listener.EntryExpiredListener[K, V] { self: PfProxy[EntryEvent[K, V]] =>
  def entryExpired(evt: core.EntryEvent[K, V]): Unit = invokeWith(EntryExpired(evt.getKey, evt.getValue)(evt))
}
private[Scala] trait EntryLoadedListener[K, V] extends map.listener.EntryLoadedListener[K, V] { self: PfProxy[EntryEvent[K, V]] =>
  def entryLoaded(evt: core.EntryEvent[K, V]): Unit = invokeWith(EntryLoaded(evt.getKey, evt.getValue)(evt))
}

private[Scala] class EntryListener[K, V](pf: PartialFunction[EntryEvent[K, V], Unit], ec: Option[ExecutionContext]) extends PfProxy(pf, ec)
  with map.listener.MapListener
  with EntryAddedListener[K, V]
  with EntryEvictedListener[K, V]
  with EntryMergedListener[K, V]
  with EntryRemovedListener[K, V]
  with EntryUpdatedListener[K, V]
  with EntryExpiredListener[K, V]
  with EntryLoadedListener[K, V]

private[Scala] trait KeyAddedListener[K] extends OnKeyAdded[K] { self: PfProxy[KeyEvent[K]] =>
  final def apply(evt: KeyAdded[K]) = invokeWith(evt)
}
private[Scala] trait KeyEvictedListener[K] extends OnKeyEvicted[K] { self: PfProxy[KeyEvent[K]] =>
  final def apply(evt: KeyEvicted[K]) = invokeWith(evt)
}
private[Scala] trait KeyMergedListener[K] extends OnKeyMerged[K] { self: PfProxy[KeyEvent[K]] =>
  final def apply(evt: KeyMerged[K]) = invokeWith(evt)
}
private[Scala] trait KeyRemovedListener[K] extends OnKeyRemoved[K] { self: PfProxy[KeyEvent[K]] =>
  final def apply(evt: KeyRemoved[K]) = invokeWith(evt)
}
private[Scala] trait KeyUpdatedListener[K] extends OnKeyUpdated[K] { self: PfProxy[KeyEvent[K]] =>
  final def apply(evt: KeyUpdated[K]) = invokeWith(evt)
}
private[Scala] trait KeyExpiredListener[K] extends OnKeyExpired[K] { self: PfProxy[KeyEvent[K]] =>
  final def apply(evt: KeyExpired[K]) = invokeWith(evt)
}
private[Scala] trait KeyLoadedListener[K] extends OnKeyLoaded[K] { self: PfProxy[KeyEvent[K]] =>
  final def apply(evt: KeyLoaded[K]) = invokeWith(evt)
}

private[Scala] class KeyListener[K](pf: PartialFunction[KeyEvent[K], Unit], ec: Option[ExecutionContext]) extends PfProxy(pf, ec)
  with map.listener.MapListener
  with KeyAddedListener[K]
  with KeyEvictedListener[K]
  with KeyMergedListener[K]
  with KeyRemovedListener[K]
  with KeyUpdatedListener[K]
  with KeyExpiredListener[K]
  with KeyLoadedListener[K]

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
final case class KeyExpired[K](key: K)(evt: core.EntryEvent[K, Object]) extends KeyEvent(evt)
final case class KeyLoaded[K](key: K)(evt: core.EntryEvent[K, Object]) extends KeyEvent(evt)

sealed abstract class EntryEvent[K, V](evt: core.EntryEvent[K, V]) extends KeyEvent(evt)
final case class EntryAdded[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryEvicted[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryRemoved[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryMerged[K, V](key: K, oldValue: Option[V], mergeValue: V, newValue: Option[V])(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryUpdated[K, V](key: K, oldValue: V, newValue: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryExpired[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)
final case class EntryLoaded[K, V](key: K, value: V)(evt: core.EntryEvent[K, V]) extends EntryEvent(evt)

final case class PartitionLost(member: core.Member, partitionId: Int)(evt: map.MapPartitionLostEvent) {
  override def toString() = evt.toString()
}

sealed trait OnKeyEvent[K] extends map.listener.MapListener
trait OnKeyAdded[K] extends OnKeyEvent[K] with map.listener.EntryAddedListener[K, Object] {
  final def entryAdded(evt: core.EntryEvent[K, Object]): Unit = apply(new KeyAdded(evt.getKey)(evt))
  def apply(evt: KeyAdded[K]): Unit
}
trait OnKeyEvicted[K] extends OnKeyEvent[K] with map.listener.EntryEvictedListener[K, Object] {
  final def entryEvicted(evt: core.EntryEvent[K, Object]): Unit = apply(new KeyEvicted(evt.getKey)(evt))
  def apply(evt: KeyEvicted[K]): Unit
}
trait OnKeyMerged[K] extends OnKeyEvent[K] with map.listener.EntryMergedListener[K, Object] {
  final def entryMerged(evt: core.EntryEvent[K, Object]): Unit = apply(new KeyMerged(evt.getKey)(evt))
  def apply(evt: KeyMerged[K]): Unit
}
trait OnKeyRemoved[K] extends OnKeyEvent[K] with map.listener.EntryRemovedListener[K, Object] {
  final def entryRemoved(evt: core.EntryEvent[K, Object]): Unit = apply(new KeyRemoved(evt.getKey)(evt))
  def apply(evt: KeyRemoved[K]): Unit
}
trait OnKeyUpdated[K] extends OnKeyEvent[K] with map.listener.EntryUpdatedListener[K, Object] {
  final def entryUpdated(evt: core.EntryEvent[K, Object]): Unit = apply(new KeyUpdated(evt.getKey)(evt))
  def apply(evt: KeyUpdated[K]): Unit
}
trait OnKeyExpired[K] extends OnKeyEvent[K] with map.listener.EntryExpiredListener[K, Object] {
  final def entryExpired(evt: core.EntryEvent[K, Object]): Unit = apply(new KeyExpired(evt.getKey)(evt))
  def apply(evt: KeyExpired[K]): Unit
}
trait OnKeyLoaded[K] extends OnKeyEvent[K] with map.listener.EntryLoadedListener[K, Object] {
  final def entryLoaded(evt: core.EntryEvent[K, Object]): Unit = apply(new KeyLoaded(evt.getKey)(evt))
  def apply(evt: KeyLoaded[K]): Unit
}

sealed trait OnEntryEvent[K, V] extends map.listener.MapListener
trait OnEntryAdded[K, V] extends OnEntryEvent[K, V] with map.listener.EntryAddedListener[K, V] {
  final def entryAdded(evt: core.EntryEvent[K, V]): Unit = apply(new EntryAdded(evt.getKey, evt.getValue)(evt))
  def apply(evt: EntryAdded[K, V]): Unit
}
trait OnEntryEvicted[K, V] extends OnEntryEvent[K, V] with map.listener.EntryEvictedListener[K, V] {
  final def entryEvicted(evt: core.EntryEvent[K, V]): Unit = apply(new EntryEvicted(evt.getKey, evt.getOldValue)(evt))
  def apply(evt: EntryEvicted[K, V]): Unit
}
trait OnEntryMerged[K, V] extends OnEntryEvent[K, V] with map.listener.EntryMergedListener[K, V] {
  final def entryMerged(evt: core.EntryEvent[K, V]): Unit = apply(new EntryMerged(evt.getKey, Option(evt.getOldValue), evt.getMergingValue, Option(evt.getValue))(evt))
  def apply(evt: EntryMerged[K, V]): Unit
}
trait OnEntryRemoved[K, V] extends OnEntryEvent[K, V] with map.listener.EntryRemovedListener[K, V] {
  final def entryRemoved(evt: core.EntryEvent[K, V]): Unit = apply(new EntryRemoved(evt.getKey, evt.getOldValue)(evt))
  def apply(evt: EntryRemoved[K, V]): Unit
}
trait OnEntryUpdated[K, V] extends OnEntryEvent[K, V] with map.listener.EntryUpdatedListener[K, V] {
  final def entryUpdated(evt: core.EntryEvent[K, V]): Unit = apply(new EntryUpdated(evt.getKey, evt.getOldValue, evt.getValue)(evt))
  def apply(evt: EntryUpdated[K, V]): Unit
}
trait OnEntryExpired[K, V] extends OnEntryEvent[K, V] with map.listener.EntryExpiredListener[K, V] {
  final def entryExpired(evt: core.EntryEvent[K, V]): Unit = apply(new EntryExpired(evt.getKey, evt.getOldValue)(evt))
  def apply(evt: EntryExpired[K, V]): Unit
}
