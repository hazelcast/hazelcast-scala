package com.hazelcast

import config._
import core._
import topic._
import map._
import collection._
import query._
import query.impl.predicates._
import query.impl._
import PredicateBuilder._
import ringbuffer.Ringbuffer

import java.lang.reflect.Method
import java.util.AbstractMap
import java.util.Map.Entry
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.implicitConversions
import scala.util.Try
import scala.util.control.NonFatal

package object Scala extends HighPriorityImplicits {
  type Freq = Int

  private[Scala] type ImmutableEntry[K, V] = AbstractMap.SimpleImmutableEntry[K, V]
  private[Scala] type MutableEntry[K, V] = AbstractMap.SimpleEntry[K, V]

  @inline implicit def fu2pfu[A](f: A => Unit): PartialFunction[A, Unit] = { case a => f(a) }
  @inline implicit def imap2scala[K, V](imap: IMap[K, V]): HzMap[K, V] = new HzMap(imap)
  @inline implicit def icoll2scala[T](coll: ICollection[T]): HzCollection[T] = new HzCollection(coll)
  @inline implicit def rb2scala[E](rb: Ringbuffer[E]): HzRingbuffer[E] = new HzRingbuffer(rb)

  implicit class HzMessage[T](private val msg: Message[T]) extends AnyVal {
    @inline def get(): T = msg.getMessageObject
  }

  implicit def toConfig(ms: MaxSize): MaxSizeConfig = ms.toConfig
  implicit def mbrConf2props(conf: Config): HzMemberProperties = new HzMemberProperties(conf)
  implicit def mbrConf2scala(conf: Config): HzConfig = new HzConfig(conf)

  implicit class HzInt(private val i: Int) extends AnyVal {
    import memory._
    def kilobytes = new MemorySize(i, MemoryUnit.KILOBYTES)
    def gigabytes = new MemorySize(i, MemoryUnit.GIGABYTES)
    def megabytes = new MemorySize(i, MemoryUnit.MEGABYTES)
    def bytes = new MemorySize(i, MemoryUnit.BYTES)
  }
  implicit class HzCDL(private val cdl: ICountDownLatch) extends AnyVal { // TODO: I can't import ICountDownLatch
    def await(dur: FiniteDuration): Boolean = cdl.await(dur.length, dur.unit)
  }

  implicit class ScalaEntry[K, V](private val entry: Entry[K, V]) extends AnyVal {
    @inline def key: K = entry.getKey
    @inline def value: V = entry.getValue
    @inline def value_=(newValue: V): Unit = entry.setValue(newValue)
  }

  implicit class HzPredicate(private val pred: Predicate[_, _]) extends AnyVal {
    def &&(other: Predicate[_, _]): Predicate[_, _] = Predicates.and(pred, other)
    def and(other: Predicate[_, _]): Predicate[_, _] = Predicates.and(pred, other)
    def ||(other: Predicate[_, _]): Predicate[_, _] = Predicates.or(pred, other)
    def or(other: Predicate[_, _]): Predicate[_, _] = Predicates.or(pred, other)
    def unary_!(): Predicate[_, _] = Predicates.not(pred)
  }

  implicit class HzMapConfig(conf: config.MapConfig) extends MapEventSubscription {
    def withTypes[K, V] = new HzTypedMapConfig[K, V](conf)
    type MSR = this.type
    def onMapEvents(localOnly: Boolean, runOn: ExecutionContext)(pf: PartialFunction[Scala.MapEvent, Unit]): MSR = {
      val mapListener = new MapListener(pf, Option(runOn))
      conf addEntryListenerConfig new config.EntryListenerConfig(mapListener, localOnly, false)
      this
    }
    def onPartitionLost(runOn: ExecutionContext)(listener: PartialFunction[PartitionLost, Unit]): MSR = {
      conf addMapPartitionLostListenerConfig new config.MapPartitionLostListenerConfig(EventSubscription.asPartitionLostListener(listener, Option(runOn)))
      this
    }
  }

  def where: EntryObject = new PredicateBuilderImpl().getEntryObject
  def where(name: String): EntryObject = new PredicateBuilderImpl().getEntryObject.get(name)
  implicit class ScalaEntryObject(private val eo: EntryObject) extends AnyVal {
    def apply(name: String): EntryObject = eo.get(name)
    def key(name: String): EntryObject = eo.key().get(name)
    def value: EntryObject = eo.get("this")
    def value_=(value: Comparable[_]): PredicateBuilder = eo.get("this").equal(value)
    def >(value: Comparable[_]): PredicateBuilder = eo.greaterThan(value)
    def <(value: Comparable[_]): PredicateBuilder = eo.lessThan(value)
    def >=(value: Comparable[_]): PredicateBuilder = eo.greaterEqual(value)
    def <=(value: Comparable[_]): PredicateBuilder = eo.lessEqual(value)
    def in(values: IterableOnce[_ <: Comparable[_]]): PredicateBuilder = eo.in(values.toSeq: _*)
    def update(name: String, value: Comparable[_]): PredicateBuilder = apply(name).equal(value)
    def <>(value: Comparable[_]): PredicateBuilder = eo.notEqual(value)
  }

  implicit class WhereString(private val sc: StringContext) extends AnyVal {
    import language.experimental.macros
    def where(args: Any*): SqlPredicate = macro Macros.Where
  }

  private[Scala] val DefaultFutureTimeout = 111.seconds
  private[Scala] implicit class ScalaFuture[T](private val f: Future[T]) extends AnyVal {
    def await: T = await(DefaultFutureTimeout)
    def await(dur: FiniteDuration): T = Await.result(f, dur)
  }

  private[Scala] implicit class JavaFuture[T](private val jFuture: java.util.concurrent.Future[T]) extends AnyVal {
    @inline def await: T = await(DefaultFutureTimeout)
    @inline def await(dur: FiniteDuration): T = if (jFuture.isDone) jFuture.get else blocking(jFuture.get(dur.length, dur.unit))
    def asScala[U](implicit ev: T => U): Future[U] = {
      if (jFuture.isDone) try Future successful jFuture.get catch { case NonFatal(t) => Future failed t }
      else {
        val callback = new FutureCallback[T, U]()
        jFuture match {
          case jFuture: CompletionStage[T] =>
            jFuture andThen callback
        }
        callback.future
      }
    }
    def asScalaOpt[U](implicit ev: T <:< U): Future[Option[U]] = {
      if (jFuture.isDone) try {
        Future successful Option(jFuture.get: U)
      } catch {
        case NonFatal(t) => Future failed t
      }
      else {
        val callback = new FutureCallback[T, Option[U]](None)(Some(_))
        callback.future
      }
    }
  }

  // Sorta naughty...
  private[this] val ClientProxy_getClient: Option[Method] = Try {
    val getClient = Class.forName("com.hazelcast.client.spi.ClientProxy").getDeclaredMethod("getClient")
    getClient.setAccessible(true)
    getClient
  }.toOption

  private[Scala] def getClientHzProxy(clientDOProxy: DistributedObject): Option[HazelcastInstance] =
    ClientProxy_getClient.map(_.invoke(clientDOProxy).asInstanceOf[HazelcastInstance])

}

package Scala {
  private[Scala] final class EntryPredicate[K, V](
      include: Entry[K, V] => Boolean, prev: Predicate[Object, Object] = null)
    extends Predicate[K, V] {
    def this(f: (K, V) => Boolean) = this(entry => f(entry.key, entry.value))
    def apply(entry: Entry[K, V]) = (prev == null || prev(entry.asInstanceOf[Entry[Object, Object]])) && include(entry)
  }
  private[Scala] final class ValuePredicate[V](
      include: V => Boolean, prev: Predicate[Object, Object] = null)
    extends Predicate[Object, V] {
    def apply(entry: Entry[Object, V]) = (prev == null || prev(entry.asInstanceOf[Entry[Object, Object]])) && include(entry.value)
  }
  private[Scala] final class KeyPredicate[K](include: K => Boolean, prev: Predicate[Object, Object] = null)
    extends Predicate[K, Object] {
    def apply(entry: Entry[K, Object]) = (prev == null || prev(entry.asInstanceOf[Entry[Object, Object]])) && include(entry.key)
  }
}
