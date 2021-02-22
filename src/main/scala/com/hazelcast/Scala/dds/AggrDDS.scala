package com.hazelcast.Scala.dds

import scala.concurrent._
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import scala.reflect.ClassTag
import collection.{Map => aMap, Set => aSet}
import collection.immutable._
import collection.IndexedSeq
import com.hazelcast.core._
import com.hazelcast.map.IMap

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
  def submit[R](
      aggregator: Aggregator[E, R],
      esOrNull: IExecutorService = null,
      tsOrNull: UserContext.Key[collection.parallel.TaskSupport] = null)(implicit ec: ExecutionContext): Future[R]
  def values()(implicit classTag: ClassTag[E], ec: ExecutionContext): Future[IndexedSeq[E]] = this submit aggr.Values[E]()
  def distinct()(implicit ec: ExecutionContext): Future[Set[E]] = this submit aggr.Distinct()
  def distribution()(implicit ec: ExecutionContext): Future[aMap[E, Freq]] = this submit aggr.Distribution()
  def count()(implicit ec: ExecutionContext): Future[Int] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[aSet[E]] = distribution().map(AggrDDS.mode)

  def maxBy[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[Option[E]] = this submit new aggr.Max(m)
  def minBy[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[Option[E]] = this submit new aggr.Min(m)
  def minMaxBy[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[Option[(E, E)]] = this submit new aggr.MinMax(m)

  def aggregate[A](
      init: => A,
      es: IExecutorService = null,
      ts: UserContext.Key[collection.parallel.TaskSupport] = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[A] = {
    val aggregator = new InlineAggregator(init _, seqop, combop)
    submit(aggregator, es, ts)
  }
  def aggregateInto[AK, A](to: IMap[AK, A], key: AK)(
      init: => A,
      es: IExecutorService = null,
      ts: UserContext.Key[collection.parallel.TaskSupport] = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[Unit] = {
    val aggregator = new InlineSavingAggregator(to.getName, key, init _, seqop, combop)
    submit(aggregator, es, ts)
  }
}

trait AggrGroupDDS[G, E] {
  def submitGrouped[AR, GR](
      aggr: Aggregator.Grouped[G, E, AR, GR],
      es: IExecutorService = null,
      ts: UserContext.Key[collection.parallel.TaskSupport] = null)(implicit ec: ExecutionContext): Future[aMap[G, GR]]

  final def submit[R](
      aggr: Aggregator[E, R],
      es: IExecutorService = null,
      ts: UserContext.Key[collection.parallel.TaskSupport] = null)(implicit ec: ExecutionContext): Future[aMap[G, R]] =
    submitGrouped(Aggregator.groupAll(aggr), es, ts)

  def distinct()(implicit ec: ExecutionContext): Future[aMap[G, Set[E]]] = submit(aggr.Distinct[E]())
  def distribution()(implicit ec: ExecutionContext): Future[aMap[G, aMap[E, Freq]]] = submit(aggr.Distribution[E]())
  def count()(implicit ec: ExecutionContext): Future[aMap[G, Int]] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[aMap[G, aSet[E]]] = distribution() map (_.mapValues(AggrDDS.mode).toMap)

  def maxBy[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[aMap[G, E]] = submitGrouped(Aggregator.groupSome(new aggr.Max(m)))
  def minBy[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[aMap[G, E]] = submitGrouped(Aggregator.groupSome(new aggr.Min(m)))
  def minMaxBy[O: Ordering](m: E => O)(implicit ec: ExecutionContext): Future[aMap[G, (E, E)]] = submitGrouped(Aggregator.groupSome(new aggr.MinMax(m)))

  def aggregate[A](
      init: => A,
      es: IExecutorService = null,
      ts: UserContext.Key[collection.parallel.TaskSupport] = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[aMap[G, A]] = {
    val aggregator = new InlineAggregator(init _, seqop, combop)
    submit(aggregator, es, ts)
  }

  def aggregateInto[A](to: IMap[G, A])(
      init: => A,
      es: IExecutorService = null,
      ts: UserContext.Key[collection.parallel.TaskSupport] = null)(seqop: (A, E) => A, combop: (A, A) => A)(implicit ec: ExecutionContext): Future[aSet[G]] = {
    val aggregator = new InlineSavingGroupAggregator[G, E, A](to.getName, init _, seqop, combop)
    submitGrouped(aggregator, es, ts).map(_.keySet)
  }

}
