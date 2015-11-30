package com.hazelcast.Scala

import scala.collection.JavaConverters._
import scala.collection.{ Map => sMap }
import scala.language.existentials
import com.hazelcast.cache
import javax.cache.processor
import com.hazelcast.cache.impl.event.CachePartitionLostEvent
import com.hazelcast.cache.impl.event.CachePartitionLostListener

final class HzCache[K, V](private val icache: cache.ICache[K, V]) extends AnyVal {

  def async = new AsyncCache(icache)

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): V = {
    val ep = new processor.EntryProcessor[K, V, V] {
      def process(entry: processor.MutableEntry[K, V], args: Object*): V = {
        val newValue = entry.getValue match {
          case null => insertIfMissing
          case oldValue => updateIfPresent(oldValue)
        }
        entry.setValue(newValue)
        newValue
      }
    }
    icache.invoke(key, ep)
  }

  def updateAndGet(key: K)(updateIfPresent: V => V): Option[V] = {
    val ep = new processor.EntryProcessor[K, V, V] {
      def process(entry: processor.MutableEntry[K, V], args: Object*): V = {
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

  def invoke[R](key: K)(args: Any*)(thunk: processor.MutableEntry[K, V] => Option[R]): Option[R] = {
    val ep = new processor.EntryProcessor[K, V, R] {
      def process(entry: processor.MutableEntry[K, V], args: Object*) = thunk(entry).asInstanceOf[Option[Object]].orNull.asInstanceOf[R]
    }
    Option(icache.invoke(key, ep, args.toArray))
  }
  def invokeAll[R](keys: Set[K])(args: Any*)(thunk: processor.MutableEntry[K, V] => Option[R]): sMap[K, R] = {
    val ep = new processor.EntryProcessor[K, V, R] {
      def process(entry: processor.MutableEntry[K, V], args: Object*) = thunk(entry).asInstanceOf[Option[Object]].orNull.asInstanceOf[R]
    }
    icache.invokeAll(keys.asJava, ep, args.toArray).asScala.mapValues(_.get)
  }
  def onPartitionLost(listener: PartialFunction[CachePartitionLostEvent, Unit]): ListenerRegistration = {
    val regId = icache addPartitionLostListener new CachePartitionLostListener {
      def partitionLost(evt: CachePartitionLostEvent) =
        if (listener isDefinedAt evt) listener(evt)
    }
    new ListenerRegistration {
      def cancel = icache removePartitionLostListener regId
    }
  }

}
