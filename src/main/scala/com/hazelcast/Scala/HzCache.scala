package com.hazelcast.Scala

import scala.collection.JavaConverters._
import scala.collection.{ Map => sMap }
import scala.language.existentials
import com.hazelcast.cache
import javax.cache.processor
import com.hazelcast.cache.impl.event.CachePartitionLostEvent
import com.hazelcast.cache.impl.event.CachePartitionLostListener
import scala.concurrent.ExecutionContext

final class HzCache[K, V](protected val icache: cache.ICache[K, V])
    extends ICacheDeltaUpdates[K, V] {

  def async = new AsyncCache(icache)

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
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartialFunction[CachePartitionLostEvent, Unit]): ListenerRegistration = {
    val regId = icache addPartitionLostListener new PfProxy(listener, Option(runOn)) with CachePartitionLostListener {
      def partitionLost(evt: CachePartitionLostEvent) = invokeWith(evt)
    }
    new ListenerRegistration {
      def cancel = icache removePartitionLostListener regId
    }
  }

}
