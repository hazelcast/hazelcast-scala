package com.hazelcast.Scala

import com.hazelcast.query.Predicate
import language.existentials

sealed trait EntryFilter[K, V]

final case class OnEntries[K, V](predicate: Predicate[_, _] = null) extends EntryFilter[K, V]
final case class OnKeys[K, V](keys: K*) extends EntryFilter[K, V]
object OnKeys {
  def apply[K, V](keys: collection.Set[K]) = new OnKeys[K, V](keys.toSeq: _*)
}
final case class OnValues[K, V](include: V => Boolean) extends EntryFilter[K, V]
