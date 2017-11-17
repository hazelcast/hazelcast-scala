package com.hazelcast.Scala

import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import scala.beans.BeanProperty
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration

import com.hazelcast.core._

final class AsyncMap[K, V] private[Scala] (protected val imap: IMap[K, V])
    extends KeyedIMapAsyncDeltaUpdates[K, V] {

  def get(key: K): Future[Option[V]] = imap.getAsync(key).asScalaOpt

  def getAll(keys: Set[K])(implicit ec: ExecutionContext): Future[Map[K, V]] = {
    val fResults = keys.iterator.map { key =>
      this.get(key).map(_.map(key -> _))
    }
    Future.sequence(fResults).map(_.flatten.toMap)
  }
  def getAllAs[R](keys: Set[K])(mf: V => R)(implicit ec: ExecutionContext): Future[Map[K, R]] = {
    val fResults = keys.iterator.map { key =>
      this.getAs(key)(mf).map(_.map(key -> _))
    }
    Future.sequence(fResults).map(_.flatten.toMap)
  }

  def put(key: K, value: V, ttl: Duration = Duration.Inf): Future[Option[V]] =
    if (ttl.isFinite && ttl.length > 0) {
      imap.putAsync(key, value, ttl.length, ttl.unit).asScalaOpt
    } else {
      imap.putAsync(key, value).asScalaOpt
    }

  def putIfAbsent(key: K, value: V, ttl: Duration = Duration.Inf): Future[Option[V]] = {
    val ep =
      if (ttl.isFinite && ttl.length > 0) {
        new AsyncMap.TTLPutIfAbsentEP(imap.getName, value, ttl.length, ttl.unit)
      } else {
        new AsyncMap.PutIfAbsentEP(value)
      }
    val callback = ep.newCallbackOpt
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def setIfAbsent(key: K, value: V, ttl: Duration = Duration.Inf): Future[Boolean] = {
    val ep =
      if (ttl.isFinite && ttl.length > 0) {
        new AsyncMap.TTLSetIfAbsentEP(imap.getName, value, ttl.length, ttl.unit)
      } else {
        new AsyncMap.SetIfAbsentEP(value)
      }
    val callback = ep.newCallback()
    imap.submitToKey(key, ep, callback)
    callback.future
  }
  private[this] val any2unit = (any: Any) => ()
  def set(key: K, value: V, ttl: Duration = Duration.Inf): Future[Unit] = {
    if (ttl.isFinite && ttl.length > 0) {
      imap.setAsync(key, value, ttl.length, ttl.unit).asScala(any2unit)
    } else {
      imap.setAsync(key, value).asScala(any2unit)
    }
  }

  def remove(key: K): Future[Option[V]] =
    imap.removeAsync(key).asScalaOpt

  def getAs[R](key: K)(map: V => R): Future[Option[R]] = {
    val ep = new AsyncMap.GetAsEP(map)
    val callback = ep.newCallbackOpt
    imap.submitToKey(key, ep, callback)
    callback.future
  }
  def getAs[C, R](getCtx: HazelcastInstance => C, key: K)(mf: (C, V) => R): Future[Option[R]] = {
    val ep = new AsyncMap.ContextGetAsEP(getCtx, mf)
    val callback = ep.newCallbackOpt
    imap.submitToKey(key, ep, callback)
    callback.future
  }

}

private[Scala] object AsyncMap {
  final class GetAsEP[V, R](val mf: V => R)
      extends SingleEntryCallbackReader[Any, V, R] {
    def onEntry(key: Any, value: V): R = {
      value match {
        case null => null.asInstanceOf[R]
        case value => mf(value)
      }
    }
  }
  final class ContextGetAsEP[C, V, R](val getCtx: HazelcastInstance => C, val mf: (C, V) => R)
      extends SingleEntryCallbackReader[Any, V, R]
      with HazelcastInstanceAware {
    @BeanProperty @transient
    var hazelcastInstance: HazelcastInstance = _
    def onEntry(key: Any, value: V): R = {
      value match {
        case null => null.asInstanceOf[R]
        case value =>
          val ctx = getCtx(hazelcastInstance)
          mf(ctx, value)
      }
    }
  }
  final class TTLPutIfAbsentEP[V](val mapName: String, val putIfAbsent: V, val ttl: Long, val unit: TimeUnit)
      extends SingleEntryCallbackReader[Any, V, V]
      with HazelcastInstanceAware {
    @BeanProperty @transient
    var hazelcastInstance: HazelcastInstance = _
    def onEntry(key: Any, existing: V): V = {
      if (existing == null) {
        val imap = hazelcastInstance.getMap[Any, V](mapName)
        imap.set(key, putIfAbsent, ttl, unit)
      }
      existing
    }
  }
  final class PutIfAbsentEP[V](val putIfAbsent: V)
      extends SingleEntryCallbackUpdater[Any, V, V] {
    def onEntry(entry: Entry[Any, V]): V = {
      val existing = entry.value
      if (existing == null) {
        entry.value = putIfAbsent
      }
      existing
    }
  }
  final class TTLSetIfAbsentEP[V](val mapName: String, val putIfAbsent: V, val ttl: Long, val unit: TimeUnit)
      extends SingleEntryCallbackReader[Any, V, Boolean]
      with HazelcastInstanceAware {
    @BeanProperty @transient
    var hazelcastInstance: HazelcastInstance = _
    def onEntry(key: Any, existing: V): Boolean = {
      val set = existing == null
      if (set) {
        val imap = hazelcastInstance.getMap[Any, V](mapName)
        imap.set(key, putIfAbsent, ttl, unit)
      }
      set
    }
  }
  final class SetIfAbsentEP[V](val putIfAbsent: V)
      extends SingleEntryCallbackUpdater[Any, V, Boolean] {
    def onEntry(entry: Entry[Any, V]): Boolean = {
      val set = entry.value == null
      if (set) {
        entry.value = putIfAbsent
      }
      set
    }
  }
}
