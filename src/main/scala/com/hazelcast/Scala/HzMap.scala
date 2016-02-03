package com.hazelcast.Scala

import java.util.Collections
import java.util.Map.Entry
import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => mMap }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.language.existentials
import com.hazelcast.Scala.dds.{ DDS, MapDDS }
import com.hazelcast.client.spi.ClientProxy
import com.hazelcast.core
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map
import com.hazelcast.map.{ AbstractEntryProcessor, MapPartitionLostEvent }
import com.hazelcast.map.listener.MapPartitionLostListener
import com.hazelcast.query.{ Predicate, PredicateBuilder, TruePredicate }
import com.hazelcast.spi.AbstractDistributedObject
import com.hazelcast.query.PagingPredicate
import java.util.Comparator
import scala.concurrent.ExecutionContext

final class HzMap[K, V](private val imap: core.IMap[K, V]) extends AnyVal {

  // Sorta naughty:
  private[Scala] def getHZ: HazelcastInstance = imap match {
    case ado: AbstractDistributedObject[_] => ado.getNodeEngine.getHazelcastInstance
    case cp: ClientProxy =>
      val getClient = classOf[ClientProxy].getDeclaredMethod("getClient")
      getClient.setAccessible(true)
      getClient.invoke(cp).asInstanceOf[HazelcastInstance]
    case _ => sys.error(s"Cannot get HazelcastInstance from ${imap.getClass}")
  }

  def async: AsyncMap[K, V] = new AsyncMap(imap)

  /**
    * NOTE: This method is generally slower than `get`
    * and also will not be part of `get` statistics.
    * It is meant for very large objects where only a
    * subset of the data is needed, thus limiting
    * unnecessary network traffic.
    */
  def getAs[R](key: K, map: V => R): Option[R] = async.getAs(key, map).await(DefaultFutureTimeout)

  def getAllAs[R](keys: Set[K], mf: V => R): mMap[K, R] = {
    val ep = new AbstractEntryProcessor[K, V](false) {
      def process(entry: Entry[K, V]): Object = {
        entry.value match {
          case null => null
          case value => mf(value).asInstanceOf[Object]
        }
      }
    }
    imap.executeOnKeys(keys.asJava, ep).asScala.asInstanceOf[mMap[K, R]]
  }

  def query[T](pred: Predicate[_, _])(mf: V => T): mMap[K, T] = {
    val ep = new AbstractEntryProcessor[K, V](false) {
      def process(entry: Entry[K, V]): Object = mf(entry.value).asInstanceOf[Object]
    }
    imap.executeOnEntries(ep, pred).asScala.asInstanceOf[mMap[K, T]]
  }

  private def updateValues(predicate: Option[Predicate[_, _]], update: V => V, returnValue: V => Object): mMap[K, V] = {
    val ep = new AbstractEntryProcessor[K, V] {
      def process(entry: Entry[K, V]): Object = {
        entry.value = update(entry.value)
        returnValue(entry.value)
      }
    }
    val map = predicate match {
      case Some(predicate) => imap.executeOnEntries(ep, predicate)
      case None => imap.executeOnEntries(ep)
    }
    map.asScala.asInstanceOf[mMap[K, V]]
  }

  def update(predicate: Predicate[_, _] = null)(updateIfPresent: V => V): Unit = {
    updateValues(Option(predicate), updateIfPresent, _ => null)
  }
  def updateAndGet(predicate: Predicate[_, _])(updateIfPresent: V => V): mMap[K, V] = {
    updateValues(Option(predicate), updateIfPresent, _.asInstanceOf[Object])
  }

  def upsertAndGet(key: K, insertIfMissing: V)(updateIfPresent: V => V): V =
    async.upsertAndGet(key, insertIfMissing)(updateIfPresent).await
  def updateAndGet(key: K)(updateIfPresent: V => V): Option[V] =
    async.updateAndGet(key)(updateIfPresent).await
  def upsert(key: K, insertIfMissing: V)(updateIfPresent: V => V): UpsertResult =
    async.upsert(key, insertIfMissing)(updateIfPresent).await
  def update(key: K)(updateIfPresent: V => V): Boolean =
    async.update(key)(updateIfPresent).await
  def set(key: K, value: V, ttl: Duration) {
    if (ttl.isFinite && ttl.length > 0) {
      imap.set(key, value, ttl.length, ttl.unit)
    } else {
      imap.set(key, value)
    }
  }

  def put(key: K, value: V, ttl: Duration): Option[V] = {
    if (ttl.isFinite && ttl.length > 0) {
      Option(imap.put(key, value, ttl.length, ttl.unit))
    } else {
      Option(imap.put(key, value))
    }
  }
  def setTransient(key: K, value: V, ttl: Duration): Unit = {
    ttl match {
      case _: FiniteDuration => imap.putTransient(key, value, ttl.length, ttl.unit)
      case _ => imap.putTransient(key, value, 0, null)
    }
  }
  def putIfAbsent(key: K, value: V, ttl: Duration): Option[V] = {
    if (ttl.isFinite && ttl.length > 0) {
      Option(imap.putIfAbsent(key, value, ttl.length, ttl.unit))
    } else {
      Option(imap.putIfAbsent(key, value))
    }
  }

  def execute[R](filter: EntryFilter[K, V])(thunk: Entry[K, V] => R): mMap[K, R] = {
      def ep = new AbstractEntryProcessor[K, V] {
        def process(entry: Entry[K, V]): Object = thunk(entry) match {
          case Unit | None | null => null
          case r: Object => r
        }
      }
    val jMap: java.util.Map[K, Object] = filter match {
      case OnValues(include) =>
        imap.executeOnEntries(ep, include)
      case OnEntries(null) =>
        imap.executeOnEntries(ep)
      case OnEntries(predicate) =>
        imap.executeOnEntries(ep, predicate)
      case OnKeys(key) =>
        val value = imap.executeOnKey(key, ep)
        Collections.singletonMap(key, value)
      case OnKeys(keys @ _*) =>
        imap.executeOnKeys(keys.toSet.asJava, ep)
    }
    jMap.asInstanceOf[java.util.Map[K, R]].asScala
  }

  def onMapEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[MapEvent, Unit]): ListenerRegistration = {
    val listener: map.listener.MapListener = new MapListener(pf, Option(runOn))
    val regId =
      if (localOnly) imap.addLocalEntryListener(listener)
      else imap.addEntryListener(listener, /* includeValue = */ false)
    new ListenerRegistration {
      def cancel(): Unit = imap.removeEntryListener(regId)
    }
  }

  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartialFunction[PartitionLossEvent, Unit]): ListenerRegistration = {
    val regId = imap addPartitionLostListener new PfProxy(listener, Option(runOn)) with MapPartitionLostListener {
      def partitionLost(evt: MapPartitionLostEvent) = invokeWith(new PartitionLossEvent(evt.getMember, evt.getPartitionId)(evt))
    }
    new ListenerRegistration {
      def cancel = imap removePartitionLostListener regId
    }
  }

  def filter(pred: PredicateBuilder): DDS[Entry[K, V]] = new MapDDS(imap, pred)
  def filter(pred: Predicate[_, _]): DDS[Entry[K, V]] = new MapDDS(imap, pred)

  // TODO: Perhaps a macro could turn this into an IndexAwarePredicate?
  def filter(f: Entry[K, V] => Boolean): DDS[Entry[K, V]] = new MapDDS(imap, new ScalaEntryPredicate(f))

  def values[O: Ordering](range: Range, pred: Predicate[_, _] = null)(sortBy: V => O, reverse: Boolean = false): Iterable[V] = {
    val pageSize = range.length
    val pageIdx = range.min / pageSize
    val dropValues = range.min % pageSize
    val comparator = new Comparator[Entry[_, V]] with Serializable {
      private[this] val ordering = implicitly[Ordering[O]] match {
        case comp if reverse => comp.reverse
        case comp => comp
      }
      def compare(a: Entry[_, V], b: Entry[_, V]): Int = ordering.compare(sortBy(a.value), sortBy(b.value))
    }.asInstanceOf[Comparator[Entry[_, _]]]
    val pp = new PagingPredicate(pred, comparator, pageSize)
    pp.setPage(pageIdx)
    val result = imap.values(pp).asScala
    if (dropValues == 0) result
    else result.drop(dropValues)
  }
  def entrySet[O: Ordering](range: Range, pred: Predicate[_, _] = null)(sortBy: Entry[K, V] => O, reverse: Boolean = false): collection.Set[Entry[K, V]] = {
    val pageSize = range.length
    val pageIdx = range.min / pageSize
    val dropEntries = range.min % pageSize
    val ordering = implicitly[Ordering[O]] match {
      case comp if reverse => comp.reverse
      case comp => comp
    }
    val comparator = new Comparator[Entry[K, V]] with Serializable {
      def compare(a: Entry[K, V], b: Entry[K, V]): Int =
        ordering.compare(sortBy(a), sortBy(b))
    }.asInstanceOf[Comparator[Entry[_, _]]]
    val pp = new PagingPredicate(pred, comparator, pageSize)
    pp.setPage(pageIdx)
    val result = imap.entrySet(pp).asScala
    if (dropEntries == 0) result
    else result.drop(dropEntries)
  }
}
