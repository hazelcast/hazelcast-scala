package com.hazelcast

import java.util.AbstractMap
import java.util.Map.Entry
import _root_.scala.concurrent.{ Await, Future, blocking }
import _root_.scala.concurrent.duration.{ DurationInt, FiniteDuration }
import _root_.scala.language.implicitConversions
import Scala._
import Scala.dds._
import cache.ICache
import core.{ Hazelcast, HazelcastInstance, ICollection, ICompletableFuture, IExecutorService, IMap, ITopic, Message }
import client.HazelcastClient
import memory.{ MemorySize, MemoryUnit }
import query.{ EntryObject, Predicate, PredicateBuilder, Predicates, SqlPredicate }
import scala.util.control.NonFatal
import ringbuffer.Ringbuffer

package Scala {

  sealed trait UpsertResult
  final case object Insert extends UpsertResult
  final case object Update extends UpsertResult

  object Macros {
    import reflect.macros.whitebox.Context
    def Where(c: Context)(args: c.Expr[Any]*): c.Expr[SqlPredicate] = {
      import c.universe._
      c.prefix.tree match {
        case Apply(_, List(Apply(_, rawParts))) =>
          val parts = rawParts map { case t @ Literal(Constant(const: String)) => (const, t.pos) }
          parts match {
            case List((raw, pos)) =>
              try {
                new SqlPredicate(raw)
                c.Expr[SqlPredicate](q" new com.hazelcast.query.SqlPredicate($raw) ")
              } catch {
                case e: RuntimeException =>
                  c.error(pos, e.getMessage)
                  c.Expr[SqlPredicate](q"")
              }
            case Nil =>
              c.abort(c.enclosingPosition, "Unknown error")
            case _ =>
              c.Expr[SqlPredicate](q" new com.hazelcast.query.SqlPredicate( StringContext(..$rawParts).raw(..$args) ) ")
          }
        case _ =>
          c.abort(c.enclosingPosition, "Unknown error")
      }
    }

  }

  trait LowPriorityImplicits {
    @inline implicit def dds2aggrDds[E](dds: DDS[E]): AggrDDS[E] = dds match {
      case dds: MapDDS[_, _, E] => new AggrMapDDS(dds)
    }
    @inline implicit def sortdds2aggrDds[E](dds: SortDDS[E]): AggrDDS[E] = dds match {
      case dds: MapSortDDS[_, _, E] => new AggrMapDDS(dds.dds, Sorted(dds.ord, dds.skip, dds.limit))
    }
    @inline implicit def dds2AggrGrpDds[G, E](dds: GroupDDS[G, E]): AggrGroupDDS[G, E] = dds match {
      case dds: MapGroupDDS[_, _, G, E] => new AggrGroupMapDDS(dds.dds)
    }
  }
  trait MediumPriorityImplicits extends LowPriorityImplicits {
    @inline implicit def dds2ordDds[O: Ordering](dds: DDS[O]): OrderingDDS[O] = dds match {
      case dds: MapDDS[_, _, O] => new OrderingMapDDS(dds)
    }
    @inline implicit def sortdds2ordDds[O: Ordering](dds: SortDDS[O]): OrderingDDS[O] = dds match {
      case dds: MapSortDDS[_, _, O] => new OrderingMapDDS(dds.dds, Sorted(dds.ord, dds.skip, dds.limit))
    }
    @inline implicit def dds2OrdGrpDds[G, O: Ordering](dds: GroupDDS[G, O]): OrderingGroupDDS[G, O] = dds match {
      case dds: MapGroupDDS[_, _, G, O] => new OrderingGroupMapDDS(dds.dds)
    }
  }
  trait HighPriorityImplicits extends MediumPriorityImplicits {
    @inline implicit def imap2dds[K, V](imap: IMap[K, V]): DDS[Entry[K, V]] = new MapDDS(imap)
    @inline implicit def inst2scala(inst: HazelcastInstance) = new HzHazelcastInstance(inst)
    @inline implicit def topic2scala[T](topic: ITopic[T]) = new HzTopic(topic)
    @inline implicit def exec2scala(exec: IExecutorService) = new HzExecutorService(exec)
    @inline implicit def icache2scala[K, V](icache: ICache[K, V]) = new HzCache[K, V](icache)
    @inline implicit def vfunc2pred[K, V](f: V => Boolean): Predicate[_, V] = new ScalaValuePredicate(f)
    @inline implicit def kfunc2pred[K, V](f: K => Boolean): Predicate[K, _] = new ScalaKeyPredicate(f)
    @inline implicit def dds2numDds[N: Numeric](dds: DDS[N]): NumericDDS[N] = dds match {
      case dds: MapDDS[_, _, N] => new NumericMapDDS(dds)
    }
    @inline implicit def sortdds2numDds[N: Numeric](dds: SortDDS[N]): NumericDDS[N] = dds match {
      case dds: MapSortDDS[_, _, N] => new NumericMapDDS(dds.dds, Sorted(dds.ord, dds.skip, dds.limit))
    }
    @inline implicit def dds2NumGrpDds[G, N: Numeric](dds: GroupDDS[G, N]): NumericGroupDDS[G, N] = dds match {
      case grpDDS: MapGroupDDS[_, _, G, N] => new NumericGroupMapDDS(grpDDS.dds)
    }
    @inline implicit def dds2entryDds[K, V](dds: DDS[Entry[K, V]]): EntryMapDDS[K, V] = dds match {
      case dds: MapDDS[K, V, Entry[K, V]] @unchecked => new EntryMapDDS(dds)
    }
    @inline implicit def imap2entryDds[K, V](imap: IMap[K, V]): EntryMapDDS[K, V] = new EntryMapDDS(new MapDDS(imap))
  }

}

package object Scala extends HighPriorityImplicits {

  type Freq = Int

  private[Scala]type ImmutableEntry[K, V] = AbstractMap.SimpleImmutableEntry[K, V]
  private[Scala]type MutableEntry[K, V] = AbstractMap.SimpleEntry[K, V]

  @inline implicit def fu2pfu[A](f: A => Unit): PartialFunction[A, Unit] = PartialFunction(f)
  @inline implicit def imap2scala[K, V](imap: IMap[K, V]) = new HzMap[K, V](imap)
  @inline implicit def icoll2scala[T](coll: ICollection[T]) = new HzCollection[T](coll)

  implicit class HzMessage[T](private val msg: Message[T]) extends AnyVal {
    @inline def get(): T = msg.getMessageObject
  }

  @inline implicit def mbrConf2props(conf: config.Config) = new HzMemberProperties(conf)
  implicit class HzConfig(private val conf: config.Config) extends AnyVal {
    def userCtx: UserContext = new UserContext(conf.getUserContext)
    def newInstance(): HazelcastInstance = Hazelcast.newHazelcastInstance(conf)
    def getInstance(): HazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(conf)
  }
  @inline implicit def clientConf2props(conf: client.config.ClientConfig) = new HzClientProperties(conf)
  implicit class HzClientConfig(private val conf: client.config.ClientConfig) extends AnyVal {
    def newClient(): HazelcastInstance = HazelcastClient.newHazelcastClient(conf)
  }

  @inline implicit def rb2scala[E](rb: Ringbuffer[E]) = new HzRingBuffer(rb)

  implicit class HzInt(private val i: Int) extends AnyVal {
    def kilobytes = new MemorySize(i, MemoryUnit.KILOBYTES)
    def gigabytes = new MemorySize(i, MemoryUnit.GIGABYTES)
    def megabytes = new MemorySize(i, MemoryUnit.MEGABYTES)
    def bytes = new MemorySize(i, MemoryUnit.BYTES)
  }

  implicit class ScalaEntry[K, V](private val entry: Entry[K, V]) extends AnyVal {
    @inline def key: K = entry.getKey
    @inline def value: V = entry.getValue
    @inline def value_=(newValue: V) = entry.setValue(newValue)
  }

  implicit class ScalaPredicate(private val pred: Predicate[_, _]) extends AnyVal {
    def &&(other: Predicate[_, _]): Predicate[_, _] = Predicates.and(pred, other)
    def and(other: Predicate[_, _]): Predicate[_, _] = Predicates.and(pred, other)
    def ||(other: Predicate[_, _]): Predicate[_, _] = Predicates.or(pred, other)
    def or(other: Predicate[_, _]): Predicate[_, _] = Predicates.or(pred, other)
    def unary_!(): Predicate[_, _] = Predicates.not(pred)
  }

  def where = new PredicateBuilder().getEntryObject
  def where(name: String) = new PredicateBuilder().getEntryObject.get(name)
  implicit class ScalaEntryObject(private val eo: EntryObject) extends AnyVal {
    def apply(name: String): EntryObject = eo.get(name)
    def key(name: String): EntryObject = eo.key().get(name)
    def value: EntryObject = eo.get("this")
    def value_=(value: Comparable[_]): PredicateBuilder = eo.get("this").equal(value)
    def >(value: Comparable[_]): PredicateBuilder = eo.greaterThan(value)
    def <(value: Comparable[_]): PredicateBuilder = eo.lessThan(value)
    def >=(value: Comparable[_]): PredicateBuilder = eo.greaterEqual(value)
    def <=(value: Comparable[_]): PredicateBuilder = eo.lessEqual(value)
    def update(name: String, value: Comparable[_]): PredicateBuilder = apply(name).equal(value)
    def update(value: Comparable[_]): PredicateBuilder = eo.equal(value)
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
          case jFuture: ICompletableFuture[T] =>
            jFuture andThen callback
        }
        callback.future
      }
    }
    def asScalaOpt[U](implicit ev: T <:< U): Future[Option[U]] = {
      if (jFuture.isDone) try Future successful Option(jFuture.get) catch { case NonFatal(t) => Future failed t }
      else {
        val callback = new FutureCallback[T, Option[U]](None)(Some(_))
        jFuture match {
          case jFuture: ICompletableFuture[T] =>
            jFuture andThen callback
        }
        callback.future
      }
    }
  }

  private[Scala] final class ScalaEntryPredicate[K, V](
    include: Entry[K, V] => Boolean, prev: Predicate[Object, Object] = null)
      extends Predicate[K, V] {
    def this(f: (K, V) => Boolean) = this(entry => f(entry.key, entry.value))
    def apply(entry: Entry[K, V]) = (prev == null || prev(entry.asInstanceOf[Entry[Object, Object]])) && include(entry)
  }
  private[Scala] final class ScalaValuePredicate[V](
    include: V => Boolean, prev: Predicate[Object, Object] = null)
      extends Predicate[Object, V] {
    def apply(entry: Entry[Object, V]) = (prev == null || prev(entry.asInstanceOf[Entry[Object, Object]])) && include(entry.value)
  }
  private[Scala] final class ScalaKeyPredicate[K](include: K => Boolean, prev: Predicate[Object, Object] = null)
      extends Predicate[K, Object] {
    def apply(entry: Entry[K, Object]) = (prev == null || prev(entry.asInstanceOf[Entry[Object, Object]])) && include(entry.key)
  }

}
