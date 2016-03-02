package com.hazelcast.Scala.dds

import scala.concurrent._
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import scala.reflect.ClassTag
import collection.{ Map => aMap, Set => aSet }
import collection.JavaConverters._
import collection.immutable._
import collection.{ Seq, IndexedSeq }
import com.hazelcast.core._

private object AggrDDS {
  def mode[E](distribution: aMap[E, Freq]): aSet[E] =
    distribution.groupBy(_._2).mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(1) match {
      case iter if iter.hasNext =>
        val (_, mode) = iter.next
        if (mode.size == distribution.size) Set.empty
        else mode
      case _ => Set.empty
    }
}

trait AggrDDS[E] {
  def submit[R](aggregator: Aggregator[E, R], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[R]
  def values()(implicit classTag: ClassTag[E], ec: ExecutionContext): Future[IndexedSeq[E]] = this submit aggr.Values[E]()
  def distinct()(implicit ec: ExecutionContext): Future[Set[E]] = this submit aggr.Distinct()
  def distribution()(implicit ec: ExecutionContext): Future[aMap[E, Freq]] = this submit aggr.Distribution()
  def count()(implicit ec: ExecutionContext): Future[Int] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[aSet[E]] = distribution().map(AggrDDS.mode)

  def max[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[Option[E]] = this submit new aggr.Max(m)
  def min[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[Option[E]] = this submit new aggr.Min(m)
  def minMax[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[Option[(E, E)]] = this submit new aggr.MinMax(m)

  def aggregate[A](init: A, es: IExecutorService = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[A] = {
    val aggregator = new Aggregator[E, A] {
      type Q = A; type W = A
      def remoteInit: Q = init
      def remoteFold(q: Q, e: E): Q = seqop(q, e)
      def remoteCombine(x: Q, y: Q): Q = combop(x, y)
      def remoteFinalize(q: Q): W = q
      def localCombine(x: W, y: W): W = combop(x, y)
      def localFinalize(w: W): A = w
    }
    submit(aggregator, es)
  }
  def aggregateInto[AK, A](to: IMap[AK, A], key: AK)(init: A, es: IExecutorService = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[Unit] = {
    val aggregator = new Aggregator[E, Unit] with HazelcastInstanceAware {
      val toMapName = to.getName
      @volatile @transient
      private[this] var toMap: HzMap[AK, A] = _
      def setHazelcastInstance(hz: HazelcastInstance): Unit = toMap = hz.getMap[AK, A](toMapName)
      type Q = A; type W = Unit
      def remoteInit: Q = init
      def remoteFold(q: Q, e: E): Q = seqop(q, e)
      def remoteCombine(x: Q, y: Q): Q = combop(x, y)
      def remoteFinalize(q: Q): W = {
        toMap.upsert(key, q)(combop(_, q))
      }
      def localCombine(x: W, y: W): W = ()
      def localFinalize(w: W) = ()
    }
    submit(aggregator, es)
  }
}

trait AggrGroupDDS[G, E] {
  def submitGrouped[AR, GR](aggr: Aggregator.Grouped[G, E, AR, GR], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[aMap[G, GR]]

  final def submit[R](aggr: Aggregator[E, R], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[aMap[G, R]] =
    submitGrouped(Aggregator.groupAll(aggr), es)

  def distinct()(implicit ec: ExecutionContext): Future[aMap[G, Set[E]]] = submit(aggr.Distinct[E]())
  def distribution()(implicit ec: ExecutionContext): Future[aMap[G, aMap[E, Freq]]] = submit(aggr.Distribution[E]())
  def count()(implicit ec: ExecutionContext): Future[aMap[G, Int]] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[aMap[G, aSet[E]]] = distribution() map (_.mapValues(AggrDDS.mode))

  def max[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[aMap[G, E]] = submitGrouped(Aggregator.groupSome(new aggr.Max(m)))
  def min[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[aMap[G, E]] = submitGrouped(Aggregator.groupSome(new aggr.Min(m)))
  def minMax[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[aMap[G, (E, E)]] = submitGrouped(Aggregator.groupSome(new aggr.MinMax(m)))

  def aggregate[A](init: A, es: IExecutorService = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[aMap[G, A]] = {
    val aggregator = new Aggregator[E, A] {
      type Q = A; type W = A
      def remoteInit: Q = init
      def remoteFold(q: Q, e: E): Q = seqop(q, e)
      def remoteCombine(x: Q, y: Q): Q = combop(x, y)
      def remoteFinalize(q: Q): W = q
      def localCombine(x: W, y: W): W = combop(x, y)
      def localFinalize(w: W): A = w
    }
    submit(aggregator, es)
  }

  def aggregateInto[A](to: IMap[G, A])(init: A, es: IExecutorService = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[aSet[G]] = {
    val aggregator = new Aggregator[E, Unit] {
      type Q = A; type W = Unit
      def remoteInit: Q = init
      def remoteFold(q: Q, e: E): Q = seqop(q, e)
      def remoteCombine(x: Q, y: Q): Q = combop(x, y)
      def remoteFinalize(q: Q): W = ()
      def localCombine(x: W, y: W): W = ()
      def localFinalize(w: W) = ()
    }
    val grouped = new Aggregator.Grouped[G, E, Unit, Unit](aggregator, PartialFunction(identity)) with HazelcastInstanceAware {
      val toMapName = to.getName
      @volatile @transient
      private[this] var toMap: HzMap[G, A] = _
      def setHazelcastInstance(hz: HazelcastInstance): Unit = toMap = hz.getMap[G, A](toMapName)
      override def remoteFinalize(q: Map[AQ]): Map[AW] = {
        q.entrySet().iterator().asScala.foreach { entry =>
          val value = entry.value.asInstanceOf[A]
          entry.value = null.asInstanceOf[AQ]
          toMap.upsert(entry.getKey, value)(combop(_, value))
        }
        q.asInstanceOf[Map[AW]]
      }
      override def localCombine(x: W, y: W): W = {
        x.putAll(y)
        x
      }
      override def localFinalize(w: W) = w.asScala.asInstanceOf[collection.mutable.Map[G, Unit]]
    }
    submitGrouped(grouped, es).map(_.keySet)
  }

}
