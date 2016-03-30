package com.hazelcast.Scala

import java.util.Map.Entry
import concurrent.Future
import language.higherKinds

private[Scala] trait DeltaUpdates[K, V] {
  type UpdateR[T]

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpdateR[V]
  def updateAndGet(key: K)(updateIfPresent: V => V): UpdateR[Option[V]]
  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpdateR[UpsertResult]
  def update(key: K)(updateIfPresent: V => V): UpdateR[Boolean]

}

private[Scala] object DeltaUpdates {
  final class UpsertEP[V](val insertIfMissing: V, val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, UpsertResult] {
    def onEntry(entry: Entry[Any, V]): UpsertResult =
      entry.value match {
        case null =>
          entry.value = insertIfMissing
          Insert
        case value =>
          entry.value = updateIfPresent(value)
          Update
      }
  }
  final class UpsertAndGetEP[V](val insertIfMissing: V, val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, V] {
    def onEntry(entry: Entry[Any, V]): V = {
      entry.value match {
        case null =>
          entry.value = insertIfMissing
          null.asInstanceOf[V] // Return `null` because we already have value locally
        case value =>
          entry.value = updateIfPresent(value)
          entry.value
      }
    }
  }
  final class UpdateEP[V](val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, Boolean] {
    def onEntry(entry: Entry[Any, V]): Boolean = {
      entry.value match {
        case null => false
        case value =>
          entry.value = updateIfPresent(value)
          true
      }
    }
  }
  final class UpdateAndGetEP[V](val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, V] {
    def onEntry(entry: Entry[Any, V]): V =
      entry.value match {
        case null => null.asInstanceOf[V]
        case value =>
          entry.value = updateIfPresent(value)
          entry.value
      }
  }
}

private[Scala] trait IMapDeltaUpdates[K, V] extends DeltaUpdates[K, V] {
  import java.util.Map.Entry
  import com.hazelcast.core.IMap

  protected def imap: IMap[K, V]

  type UpdateR[T] = T

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpdateR[V] =
    imap.async.upsertAndGet(key, insertIfMissing)(updateIfPresent).await
  def updateAndGet(key: K)(updateIfPresent: V => V): UpdateR[Option[V]] =
    imap.async.updateAndGet(key)(updateIfPresent).await
  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpdateR[UpsertResult] =
    imap.async.upsert(key, insertIfMissing)(updateIfPresent).await
  def update(key: K)(updateIfPresent: V => V): UpdateR[Boolean] =
    imap.async.update(key)(updateIfPresent).await

}

private[Scala] trait IMapAsyncDeltaUpdates[K, V] extends DeltaUpdates[K, V] {
  import com.hazelcast.core.IMap

  protected def imap: IMap[K, V]

  type UpdateR[T] = Future[T]

  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): Future[UpsertResult] = {
    val ep = new DeltaUpdates.UpsertEP(insertIfMissing, updateIfPresent)
    val callback = ep.newCallback()
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): Future[V] = {
    val ep = new DeltaUpdates.UpsertAndGetEP(insertIfMissing, updateIfPresent)
    val callback = ep.newCallback(insertIfMissing)
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def updateAndGet(key: K)(updateIfPresent: V => V): Future[Option[V]] = {
    val ep = new DeltaUpdates.UpdateAndGetEP(updateIfPresent)
    val callback = ep.newCallbackOpt
    imap.submitToKey(key, ep, callback)
    callback.future
  }

  def update(key: K)(updateIfPresent: V => V): Future[Boolean] = {
    val ep = new DeltaUpdates.UpdateEP(updateIfPresent)
    val callback = ep.newCallback()
    imap.submitToKey(key, ep, callback)
    callback.future
  }

}
