package com.hazelcast.Scala.jcache

import scala.collection.JavaConverters._
import scala.collection.{ Map => sMap }
import scala.concurrent.ExecutionContext

import com.hazelcast.Scala._
import com.hazelcast.cache
import com.hazelcast.cache.impl.event.CachePartitionLostEvent
import com.hazelcast.cache.impl.event.CachePartitionLostListener
import com.hazelcast.core.IExecutorService

final class HzCache[K, V](icache: cache.ICache[K, V]) {
  import javax.cache._
  import processor._

  def async = new AsyncCache(icache)

  def invoke[R](key: K)(thunk: MutableEntry[K, V] => Option[R]): Option[R] = {
    val ep = new CacheEntryProcessor[K, V, R] {
      def process(entry: MutableEntry[K, V], args: Object*) = thunk(entry).asInstanceOf[Option[Object]].orNull.asInstanceOf[R]
    }
    Option(icache.invoke(key, ep))
  }
  def invokeAll[R](keys: Set[K])(thunk: MutableEntry[K, V] => Option[R]): sMap[K, R] = {
    val ep = new CacheEntryProcessor[K, V, R] {
      def process(entry: MutableEntry[K, V], args: Object*) = thunk(entry).asInstanceOf[Option[Object]].orNull.asInstanceOf[R]
    }
    icache.invokeAll(keys.asJava, ep).asScala.mapValues(_.get).toMap
  }
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartialFunction[CachePartitionLostEvent, Unit]): ListenerRegistration = {
    val regId = icache addPartitionLostListener new PfProxy(listener, Option(runOn)) with CachePartitionLostListener {
      def partitionLost(evt: CachePartitionLostEvent) = invokeWith(evt)
    }
    new ListenerRegistration {
      def cancel = icache removePartitionLostListener regId
    }
  }

  type UpdateR[T] = T

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): V = {
    require(insertIfMissing != null, "Insert value cannot be null")
    val ep = new CacheEntryProcessor[K, V, V] {
      def process(entry: MutableEntry[K, V], args: Object*): V = {
        entry.getValue match {
          case null =>
            entry setValue insertIfMissing
            null.asInstanceOf[V]
          case oldValue =>
            entry setValue updateIfPresent(oldValue)
            entry.getValue
        }
      }
    }
    icache.invoke(key, ep) match {
      case null => insertIfMissing
      case value => value
    }
  }

  def updateAndGet(key: K)(updateIfPresent: V => V): Option[V] = {
    val ep = new CacheEntryProcessor[K, V, V] {
      def process(entry: MutableEntry[K, V], args: Object*): V = {
        entry.getValue match {
          case null => null.asInstanceOf[V]
          case oldValue =>
            val newValue = updateIfPresent(oldValue)
            entry.setValue(newValue)
            newValue
        }
      }
    }
    Option(icache.invoke(key, ep))
  }
  def updateAndGetIf(cond: V => Boolean, key: K)(updateIfPresent: V => V): Option[V] = {
    val ep = new CacheEntryProcessor[K, V, V] {
      def process(entry: MutableEntry[K, V], args: Object*): V = {
        entry.getValue match {
          case null => null.asInstanceOf[V]
          case oldValue if cond(oldValue) =>
            val newValue = updateIfPresent(oldValue)
            entry.setValue(newValue)
            newValue
          case _ => null.asInstanceOf[V]
        }
      }
    }
    Option(icache.invoke(key, ep))
  }

  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpsertResult = {
    val ep = new CacheEntryProcessor[K, V, UpsertResult] {
      def process(entry: MutableEntry[K, V], args: Object*): UpsertResult = {
        entry.getValue match {
          case null =>
            entry setValue insertIfMissing
            WasInserted
          case oldValue =>
            entry setValue updateIfPresent(oldValue)
            WasUpdated
        }
      }
    }
    icache.invoke(key, ep)
  }

  def update(key: K, runOn: IExecutorService)(updateIfPresent: V => V): Boolean = {
    val ep = new CacheEntryProcessor[K, V, Object] {
      def process(entry: MutableEntry[K, V], args: Object*): Object = {
        entry.getValue match {
          case null =>
            java.lang.Boolean.FALSE
          case value =>
            entry setValue updateIfPresent(value)
            java.lang.Boolean.TRUE
        }
      }
    }
    icache.invoke(key, ep).asInstanceOf[Boolean]
  }
  def updateIf(cond: V => Boolean, key: K, runOn: IExecutorService)(updateIfPresent: V => V): Boolean = {
    val ep = new CacheEntryProcessor[K, V, Object] {
      def process(entry: MutableEntry[K, V], args: Object*): Object = {
        entry.getValue match {
          case null =>
            java.lang.Boolean.FALSE
          case value if cond(value) =>
            entry setValue updateIfPresent(entry.getValue)
            java.lang.Boolean.TRUE
          case _ =>
            java.lang.Boolean.FALSE
        }
      }
    }
    icache.invoke(key, ep).asInstanceOf[Boolean]
  }
  def getAndUpsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): Option[V] = {
    val ep = new CacheEntryProcessor[K, V, V] {
      def process(entry: MutableEntry[K, V], args: Object*): V = {
        entry.getValue match {
          case null =>
            entry.setValue(insertIfMissing)
            null.asInstanceOf[V]
          case value =>
            entry.setValue(updateIfPresent(value))
            value
        }
      }
    }
    Option(icache.invoke(key, ep))
  }
  def getAndUpdate(key: K)(updateIfPresent: V => V): Option[V] = {
    val ep = new CacheEntryProcessor[K, V, V] {
      def process(entry: MutableEntry[K, V], args: Object*): V = {
        entry.getValue match {
          case null => null.asInstanceOf[V]
          case value =>
            entry.setValue(updateIfPresent(value))
            value
        }
      }
    }
    Option(icache.invoke(key, ep))
  }
  def getAndUpdateIf(cond: V => Boolean, key: K)(updateIfPresent: V => V): Option[(V, Boolean)] = {
    val ep = new CacheEntryProcessor[K, V, Object] {
      def process(entry: MutableEntry[K, V], args: Object*): Object = {
        entry.getValue match {
          case null => null
          case value if cond(value) =>
            entry.setValue(updateIfPresent(value))
            (value, true)
          case value =>
            (value, false)
        }
      }
    }
    Option(icache.invoke(key, ep).asInstanceOf[(V, Boolean)])
  }
}
