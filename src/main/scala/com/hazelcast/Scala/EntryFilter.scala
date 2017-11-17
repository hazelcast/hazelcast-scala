package com.hazelcast.Scala

import com.hazelcast.query.Predicate
import collection.mutable.Map
import language.{higherKinds, existentials}

sealed trait EntryFilter[K, V] {
  type EV
  type M[R]
}

final case class OnEntries[K, V](predicate: Predicate[_, _] = null) extends EntryFilter[K, V] {
  type EV = V
  type M[R] = Map[K, R]
}
final case class OnValues[K, V](include: V => Boolean) extends EntryFilter[K, V] {
  type EV = V
  type M[R] = Map[K, R]
}
final case class OnKeys[K, V](keys: collection.Set[K]) extends EntryFilter[K, V] {
  type EV = V
  type M[R] = Map[K, R]
}
object OnKeys {
  def apply[K, V](key1: K, key2: K, keyn: K*) = new OnKeys[K, V](keyn.toSet + key1 + key2)
}

final case class OnKey[K, V](key: K) extends EntryFilter[K, V] {
  type EV = Option[V]
  type M[R] = R
}
