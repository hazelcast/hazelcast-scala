//package com.hazelcast.Scala
//
//import collection.JavaConverters._
//import com.hazelcast.core.HazelcastInstance
//import com.hazelcast.core.IMap
//import scala.concurrent._
//import scala.concurrent.ExecutionContext.Implicits.global
//
//private[Scala] sealed trait Join[V, JK, JV] {
//  type T
//  protected type R
//  type CB[A] = (A, V, (A, T) => A) => A
//  def mapName: String
//  def init[A](hz: HazelcastInstance): CB[A] = {
//    val joinMap = hz.getMap[JK, JV](mapName)
//    join(joinMap) _
//  }
//  def join[A](joinMap: IMap[JK, JV])(acc: A, value: V, callback: (A, T) => A): A
//}
//
//private[Scala] final case class InnerOne[V, JK, JV](mapName: String, on: V => Option[JK]) extends Join[V, JK, JV] {
//  type T = (V, JV)
//  type R = Option[T]
//  def join[A](joinMap: IMap[JK, JV])(acc: A, value: V, callback: (A, T) => A): A = {
//    on(value) match {
//      case None => acc
//
//    }
//  }
//  def join(joinMap: IMap[JK, JV])(value: V): Future[R] = {
//    on(value) match {
//      case None => Future successful None
//      case Some(fk) => joinMap.async.get(fk).map { res =>
//        res.map(value -> _)
//      }
//    }
//  }
//  def callback[A](acc: A, value: Future[R])(callback: (A, T) => A): A = {
//    value.await match {
//      case None => acc
//      case Some(t) => callback(acc, t)
//    }
//  }
//}
//private[Scala] final case class OuterOne[V, JK, JV](mapName: String, on: V => Option[JK]) extends Join[V, JK, JV] {
//  type T = (V, Option[JV])
//  type R = T
//  def join(joinMap: IMap[JK, JV])(value: V): Future[R] = {
//    on(value) match {
//      case None => Future successful (value, None)
//      case Some(fk) =>
//        joinMap.async.get(fk).map { res =>
//          value -> res
//        }
//    }
//  }
//  def callback[A](acc: A, value: Future[R])(callback: (A, T) => A): A = {
//    val t = value.await
//    callback(acc, t)
//  }
//}
//private[Scala] final case class InnerMany[V, JK, JV](mapName: String, on: V => Set[JK]) extends Join[V, JK, JV] {
//  type T = (V, Map[JK, JV])
//  type R = Option[T]
//  def join(joinMap: IMap[JK, JV])(value: V): Future[R] = {
//    val fks = on(value)
//    if (fks.isEmpty) Future successful None
//    else {
//      joinMap.async.getAll(fks).map { result =>
//        if (result.isEmpty) None
//        else Some(value -> result)
//      }
//    }
//  }
//  def callback[A](acc: A, value: Future[R])(callback: (A, T) => A): A = {
//    value.await match {
//      case None => acc
//      case Some(t) => callback(acc, t)
//    }
//  }
//}
//private[Scala] final case class OuterMany[V, JK, JV](mapName: String, on: V => Set[JK]) extends Join[V, JK, JV] {
//  type T = (V, Map[JK, JV])
//  type R = T
//  def join(joinMap: IMap[JK, JV])(value: V): Future[R] = {
//    val fks = on(value)
//    if (fks.isEmpty) Future successful value -> Map.empty
//    else {
//      joinMap.async.getAll(fks).map(value -> _)
//    }
//  }
//  def callback[A](acc: A, value: Future[R])(callback: (A, T) => A): A = {
//    val t = value.await
//    callback(acc, t)
//  }
//}
