package com.hazelcast.Scala

import scala.collection.{Map, Set}
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.nio.Address

private[Scala] sealed trait Join[V, JK, JV] {
  type T
  protected type R
  type CB[A] = (A, V, (A, T) => A) => A
  def mapName: String
  def init[A](hz: HazelcastInstance, callerAddress:Option[Address]): CB[A] = {
    val joinMap = hz.getMap[JK, JV](mapName)
    join(new CachingMap(joinMap), callerAddress) _
  }
  def join[A](joinMap: CachingMap[JK, JV], callerAddress:Option[Address])(acc: A, value: V, callback: (A, T) => A): A
}

private[Scala] final case class InnerOne[V, JK, JV](mapName: String, on: V => JK) extends Join[V, JK, JV] {
  type T = (V, JV)
  type R = Option[T]
  def join[A](joinMap: CachingMap[JK, JV], callerAddress:Option[Address])(acc: A, value: V, callback: (A, T) => A): A = {
    val jk = on(value)
    joinMap.get(jk, callerAddress) match {
      case None => acc
      case Some(jv) => callback(acc, value -> jv)
    }
  }
}

private[Scala] final case class OuterOne[V, JK, JV](mapName: String, on: V => JK) extends Join[V, JK, JV] {
  type T = (V, Option[JV])
  type R = T
  def join[A](joinMap: CachingMap[JK, JV], callerAddress:Option[Address])(acc: A, value: V, callback: (A, T) => A): A = {
    val jk = on(value)
    val jv = joinMap.get(jk, callerAddress)
    callback(acc, value -> jv)
  }
}

private[Scala] final case class InnerMany[V, JK, JV](mapName: String, on: V => Set[JK]) extends Join[V, JK, JV] {
  type T = (V, Map[JK, JV])
  type R = Option[T]
  def join[A](joinMap: CachingMap[JK, JV], callerAddress:Option[Address])(acc: A, value: V, callback: (A, T) => A): A = {
    val fks = on(value)
    if (fks.isEmpty) acc
    else {
      val jvs = joinMap getAll(fks, callerAddress)
      if (jvs.isEmpty) acc
      else callback(acc, value -> jvs)
    }
  }
}
private[Scala] final case class OuterMany[V, JK, JV](mapName: String, on: V => Set[JK]) extends Join[V, JK, JV] {
  type T = (V, Map[JK, JV])
  type R = T
  def join[A](joinMap: CachingMap[JK, JV], callerAddress:Option[Address])(acc: A, value: V, callback: (A, T) => A): A = {
    val fks = on(value)
    val fvs = if (fks.isEmpty) Map.empty[JK, JV] else joinMap.getAll(fks, callerAddress)
    callback(acc, value -> fvs)
  }
}

private class CachingMap[K, V](imap: IMap[K, V]) {
  private[this] val cmap = new java.util.concurrent.ConcurrentHashMap[K, Option[V]](64)

  def get(key: K, callerAddress:Option[Address]): Option[V] = {
    cmap.get(key) match {
      case null =>
        val option = Option(imap.getFastIfLocal(key, callerAddress))
        cmap.putIfAbsent(key, option) match {
          case null => option
          case option => option
        }
      case result => result
    }
  }
  def getAll(keys: Set[K], callerAddress:Option[Address]): Map[K, V] = {
    val (cached, notCached) = keys.iterator.map(k => k -> cmap.get(k)).partition(_._2 != null)
    val all =
      if (notCached.isEmpty) cached
      else {
        val keysNotCached = notCached.map(_._1).toSet
        val keysNotCachedByPartition = imap.getHZ.groupByPartitionId(keysNotCached)
        val found =
          imap.getFastIfLocal(keysNotCachedByPartition.toIterable.par, callerAddress)
            .seq.iterator.foldLeft(new collection.mutable.HashMap[K, V]) {
              case (found, entry) =>
                found.update(entry.key, entry.value)
                found
            }
        val keysNotFound = found.keySet.diff(keysNotCached)
        keysNotFound.foreach { key =>
          cmap.putIfAbsent(key, None) match {
            case Some(value) => // Very unlikely, but... consistency
              found.put(key, value)
            case _ => // Ignore
          }
        }
        val foundAndCached = found.iterator.map {
          case (key, value) =>
            val someValue = Some(value)
            cmap.putIfAbsent(key, someValue) match {
              case null => key -> someValue
              case other => key -> other
            }
        }
        foundAndCached ++ cached
      }
    all.collect {
      case (key, Some(value)) => key -> value
    }.toMap
  }
}
