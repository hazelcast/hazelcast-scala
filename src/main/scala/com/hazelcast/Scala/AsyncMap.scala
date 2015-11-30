package com.hazelcast.Scala

import java.util.Map.Entry
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import com.hazelcast.core.{ IExecutorService, IMap, Member }
import com.hazelcast.query.Predicate
import scala.concurrent.ExecutionContext

final class AsyncMap[K, V] private[Scala] (private val imap: IMap[K, V]) extends AnyVal {

  def get(key: K): Future[Option[V]] = imap.getAsync(key).asScalaOpt

  def getAll(keys: Set[K])(implicit ec: ExecutionContext): Future[Map[K, V]] = {
    val fResults = keys.iterator.map { key =>
      this.get(key).map(_.map(key -> _))
    }
    Future.sequence(fResults).map(_.flatten.toMap)
  }
  def getAllAs[R](keys: Set[K], mf: V => R)(implicit ec: ExecutionContext): Future[Map[K, R]] = {
    val fResults = keys.iterator.map { key =>
      this.getAs(key, mf).map(_.map(key -> _))
    }
    Future.sequence(fResults).map(_.flatten.toMap)
  }

  def put(key: K, value: V, ttl: Duration = Duration.Zero): Future[Option[V]] =
    if (ttl.isFinite && ttl.length > 0) {
      imap.putAsync(key, value, ttl.length, ttl.unit).asScalaOpt
    } else {
      imap.putAsync(key, value).asScalaOpt
    }

  def remove(key: K): Future[Option[V]] =
    imap.removeAsync(key).asScalaOpt

  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): Future[UpsertResult] = {
    val ep = new CallbackEntryUpdater[K, V, UpsertResult] {
      def onEntry(entry: Entry[K, V]): UpsertResult =
        entry.value match {
          case null =>
            entry.value = insertIfMissing
            Insert
          case value =>
            entry.value = updateIfPresent(value)
            Update
        }
    }
    val callback = ep.newCallback()
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): Future[V] = {
    val ep = new CallbackEntryUpdater[K, V, V] {
      def onEntry(entry: Entry[K, V]): V = {
        entry.value match {
          case null =>
            entry.value = insertIfMissing
            null.asInstanceOf[V]
          case value =>
            entry.value = updateIfPresent(value)
            entry.value
        }
      }
    }
    val callback = ep.newCallback(insertIfMissing)
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def updateAndGet(key: K)(updateIfPresent: V => V): Future[Option[V]] = {
    val ep = new CallbackEntryUpdater[K, V, V] {
      def onEntry(entry: Entry[K, V]): V =
        entry.value match {
          case null => null.asInstanceOf[V]
          case value =>
            entry.value = updateIfPresent(value)
            entry.value
        }
    }
    val callback = ep.newCallbackOpt
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def update(key: K)(updateIfPresent: V => V): Future[Boolean] = {
    val ep = new CallbackEntryUpdater[K, V, Boolean] {
      def onEntry(entry: Entry[K, V]): Boolean = {
        entry.value match {
          case null => false
          case value =>
            entry.value = updateIfPresent(value)
            true
        }
      }
    }
    val callback = ep.newCallback()
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def getAs[R](key: K, map: V => R): Future[Option[R]] = {
    val ep = new CallbackEntryReader[K, V, R] {
      def onEntry(key: K, value: V): R = {
        value match {
          case null => null.asInstanceOf[R]
          case value => map(value)
        }
      }
    }
    val callback = ep.newCallbackOpt
    imap.submitToKey(key, ep, callback)
    callback.future
  }

}
