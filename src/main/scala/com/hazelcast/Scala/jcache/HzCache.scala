package com.hazelcast.Scala.jcache

import scala.collection.JavaConverters._
import scala.collection.{ Map => sMap }
import scala.concurrent.ExecutionContext

import com.hazelcast.Scala._
import com.hazelcast.cache
import com.hazelcast.cache.impl.event.CachePartitionLostEvent
import com.hazelcast.cache.impl.event.CachePartitionLostListener

final class HzCache[K, V](icache: cache.ICache[K, V]) extends KeyedDeltaUpdates[K, V] {
  import javax.cache._
  import processor._

  def async = new AsyncCache(icache)

  def invoke[R](key: K)(thunk: MutableEntry[K, V] => Option[R]): Option[R] = {
    val ep = new EntryProcessor[K, V, R] {
      def process(entry: MutableEntry[K, V], args: Object*) = thunk(entry).asInstanceOf[Option[Object]].orNull.asInstanceOf[R]
    }
    Option(icache.invoke(key, ep))
  }
  def invokeAll[R](keys: Set[K])(thunk: MutableEntry[K, V] => Option[R]): sMap[K, R] = {
    val ep = new EntryProcessor[K, V, R] {
      def process(entry: MutableEntry[K, V], args: Object*) = thunk(entry).asInstanceOf[Option[Object]].orNull.asInstanceOf[R]
    }
    icache.invokeAll(keys.asJava, ep).asScala.mapValues(_.get)
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
