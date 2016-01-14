package com.hazelcast.Scala.dds

import scala.concurrent._
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import scala.reflect.ClassTag
import collection.{ Map => aMap, Set => aSet }
import collection.immutable._
import collection.{ Seq, IndexedSeq }
import com.hazelcast.core.IExecutorService

private[dds] object AggrDDS {
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
  def fetch()(implicit classTag: ClassTag[E], ec: ExecutionContext): Future[IndexedSeq[E]] = this submit aggr.Fetch[E]()
  def distinct()(implicit ec: ExecutionContext): Future[Set[E]] = this submit aggr.Distinct()
  def distribution()(implicit ec: ExecutionContext): Future[aMap[E, Freq]] = this submit aggr.Distribution()
  def count()(implicit ec: ExecutionContext): Future[Int] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[aSet[E]] = distribution().map(AggrDDS.mode)
}

trait AggrGroupDDS[G, E] {
  def submitGrouped[AR, GR](aggr: Aggregator.Grouped[G, E, AR, GR], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[aMap[G, GR]]

  final def submit[R](aggr: Aggregator[E, R], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[aMap[G, R]] =
    submitGrouped(Aggregator.groupAll(aggr), es)
  def distinct()(implicit ec: ExecutionContext): Future[aMap[G, Set[E]]] = submit(aggr.Distinct[E]())
  def distribution()(implicit ec: ExecutionContext): Future[aMap[G, aMap[E, Freq]]] = submit(aggr.Distribution[E]())
  def count()(implicit ec: ExecutionContext): Future[aMap[G, Int]] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[aMap[G, aSet[E]]] = distribution() map (_.mapValues(AggrDDS.mode))
}
