package com.hazelcast.Scala

import concurrent.Future
import language.higherKinds

private[Scala] trait DeltaUpdates[K, V] {
  type UpdateR[T]

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpdateR[V]
  def updateAndGet(key: K)(updateIfPresent: V => V): UpdateR[Option[V]]
  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpdateR[UpsertResult]
  def update(key: K)(updateIfPresent: V => V): UpdateR[Boolean]

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
  import java.util.Map.Entry
  import com.hazelcast.core.IMap

  protected def imap: IMap[K, V]

  type UpdateR[T] = Future[T]

  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): Future[UpsertResult] = {
    val ep = new SingleEntryCallbackUpdater[K, V, UpsertResult] {
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
    val ep = new SingleEntryCallbackUpdater[K, V, V] {
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
    val ep = new SingleEntryCallbackUpdater[K, V, V] {
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
    val ep = new SingleEntryCallbackUpdater[K, V, Boolean] {
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

}

private[Scala] trait ICacheDeltaUpdates[K, V] extends DeltaUpdates[K, V] {
  import com.hazelcast.cache.ICache
  import javax.cache.processor._

  protected def icache: ICache[K, V]

  type UpdateR[T] = T

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): V = {
    val ep = new EntryProcessor[K, V, Object] with Serializable {
      def process(entry: MutableEntry[K, V], args: Object*): Object = {
        val newValue = entry.getValue match {
          case null => insertIfMissing
          case oldValue => updateIfPresent(oldValue)
        }
        entry.setValue(newValue)
        newValue.asInstanceOf[Object]
      }
    }
    icache.invoke(key, ep).asInstanceOf[V]
  }

  def updateAndGet(key: K)(updateIfPresent: V => V): Option[V] = {
    val ep = new EntryProcessor[K, V, Object] with Serializable {
      def process(entry: MutableEntry[K, V], args: Object*): Object = {
        entry.getValue match {
          case null => null
          case oldValue =>
            val newValue = updateIfPresent(oldValue)
            entry.setValue(newValue)
            newValue.asInstanceOf[Object]
        }
      }
    }
    Option(icache.invoke(key, ep).asInstanceOf[V])
  }

  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpsertResult = {
    val ep = new EntryProcessor[K, V, Object] with Serializable {
      def process(entry: MutableEntry[K, V], args: Object*): Object = {
        entry.getValue match {
          case null =>
            entry setValue insertIfMissing
            Insert
          case oldValue =>
            entry setValue updateIfPresent(oldValue)
            Update
        }
      }
    }
    icache.invoke(key, ep).asInstanceOf[UpsertResult]
  }
  def update(key: K)(updateIfPresent: V => V): Boolean = {
    val ep = new EntryProcessor[K, V, Object] with Serializable {
      def process(entry: MutableEntry[K, V], args: Object*): Object = {
        entry.getValue match {
          case null =>
            Boolean box false
          case value =>
            entry setValue updateIfPresent(entry.getValue)
            Boolean box true
        }
      }
    }
    icache.invoke(key, ep).asInstanceOf[Boolean]
  }

}
