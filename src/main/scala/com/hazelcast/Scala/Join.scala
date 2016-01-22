package com.hazelcast.Scala

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.concurrent._

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap

private[Scala] sealed trait Join[V, JK, JV] {
  type T
  protected type R
  type CB[A] = (A, V, (A, T) => A) => A
  def mapName: String
  def init[A](hz: HazelcastInstance): CB[A] = {
    val joinMap = hz.getMap[JK, JV](mapName)
    join(joinMap) _
  }
  def join[A](joinMap: IMap[JK, JV])(acc: A, value: V, callback: (A, T) => A): A
}

private[Scala] final case class InnerOne[V, JK, JV](mapName: String, on: V => Option[JK]) extends Join[V, JK, JV] {
  type T = (V, JV)
  type R = Option[T]
  def join[A](joinMap: IMap[JK, JV])(acc: A, value: V, callback: (A, T) => A): A = {
    on(value) match {
      case None => acc
      case Some(jk) =>
        blocking(joinMap get jk) match {
          case null => acc
          case jv => callback(acc, value -> jv)
        }
    }
  }
}

private[Scala] final case class OuterOne[V, JK, JV](mapName: String, on: V => Option[JK]) extends Join[V, JK, JV] {
  type T = (V, Option[JV])
  type R = T
  def join[A](joinMap: IMap[JK, JV])(acc: A, value: V, callback: (A, T) => A): A = {
    val jv = on(value).flatMap(jk => Option(blocking(joinMap get jk)))
    callback(acc, value -> jv)
  }
}

private[Scala] final case class InnerMany[V, JK, JV](mapName: String, on: V => Set[JK]) extends Join[V, JK, JV] {
  type T = (V, Map[JK, JV])
  type R = Option[T]
  def join[A](joinMap: IMap[JK, JV])(acc: A, value: V, callback: (A, T) => A): A = {
    val fks = on(value)
    if (fks.isEmpty) acc
    else {
      val jvs = blocking(joinMap getAll fks.asJava)
      if (jvs.isEmpty) acc
      else callback(acc, value -> jvs.asScala)
    }
  }
}
private[Scala] final case class OuterMany[V, JK, JV](mapName: String, on: V => Set[JK]) extends Join[V, JK, JV] {
  type T = (V, Map[JK, JV])
  type R = T
  def join[A](joinMap: IMap[JK, JV])(acc: A, value: V, callback: (A, T) => A): A = {
    val fks = on(value)
    val fvs = if (fks.isEmpty) Map.empty[JK, JV] else blocking(joinMap getAll fks.asJava).asScala
    callback(acc, value -> fvs)
  }
}
