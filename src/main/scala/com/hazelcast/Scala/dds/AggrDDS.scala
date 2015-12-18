package com.hazelcast.Scala.dds

import scala.concurrent._
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import scala.reflect.ClassTag
import collection.{ Map => cMap }
import collection.immutable._
import collection.Seq
import com.hazelcast.core.IExecutorService

private[dds] object AggrDDS {
  def sortByFreq[E](dist: cMap[E, Freq], top: Int): SortedMap[Freq, Set[E]] = {
    assert(top != 0)
    dist.foldLeft(new TreeMap[Int, Set[E]]()(Ordering[Int].reverse)) {
      case (tmap, (value, freq)) =>
        if (tmap.isEmpty) tmap.updated(freq, Set(value))
        else if (freq >= tmap.last._1 || top < 0) {
          val updated = tmap.get(freq) match {
            case None => tmap.updated(freq, Set(value))
            case Some(values) => tmap.updated(freq, values + value)
          }
          if (top > 0) updated.take(top) else updated
        } else tmap
    }
  }
  def mode[E](distribution: cMap[E, Freq]): Set[E] = {
    sortByFreq(distribution, 1).headOption match {
      case None => Set.empty
      case Some((_, mode)) =>
        if (mode.size == distribution.size) Set.empty
        else mode
    }
  }
}

trait AggrDDS[E] {
  def submit[Q, W, R](aggregator: (Option[Ordering[E]], Option[Int], Option[Int]) => Aggregator[Q, E, W, R], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[R]
  def fetch()(implicit classTag: ClassTag[E], ec: ExecutionContext): Future[Seq[E]] = submit(new aggr.Fetch)
  def distinct()(implicit ec: ExecutionContext): Future[Set[E]] = this submit aggr.Distinct()
  def distribution()(implicit ec: ExecutionContext): Future[cMap[E, Freq]] = this submit aggr.Distribution()
  def count()(implicit ec: ExecutionContext): Future[Int] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[Set[E]] = distribution().map(AggrDDS.mode)
  def frequency(top: Int = -1)(implicit ec: ExecutionContext): Future[SortedMap[Freq, Set[E]]] = {
    if (top == 0) Future successful SortedMap.empty
    else distribution().map(AggrDDS.sortByFreq(_, top))
  }
}

trait AggrGroupDDS[G, E] {
  def submit[Q, W, AR, GR](aggr: Aggregation.GroupAggregation[G, Q, E, W, AR, GR], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[cMap[G, GR]]

  def submitGrouped[Q, W, R](aggr: Aggregation[Q, E, W, R], es: IExecutorService)(implicit ec: ExecutionContext): Future[cMap[G, R]] =
    submit[Q, W, R, R](Aggregation.groupAll(aggr), es)
  def submitGrouped[Q, W, R](aggr: Aggregation[Q, E, W, R])(implicit ec: ExecutionContext): Future[cMap[G, R]] =
    submit[Q, W, R, R](Aggregation.groupAll(aggr))

  def distinct()(implicit ec: ExecutionContext): Future[cMap[G, Set[E]]] = submitGrouped(aggr.Distinct[E]())
  def distribution()(implicit ec: ExecutionContext): Future[cMap[G, cMap[E, Freq]]] = submitGrouped(aggr.Distribution[E]())
  def count()(implicit ec: ExecutionContext): Future[cMap[G, Int]] = submitGrouped(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[cMap[G, Set[E]]] = distribution() map (_.mapValues(AggrDDS.mode))
  def frequency(top: Int = -1)(implicit ec: ExecutionContext): Future[cMap[G, SortedMap[Freq, Set[E]]]] = {
    if (top == 0) Future successful cMap.empty
    else distribution().map(_.mapValues(AggrDDS.sortByFreq(_, top)))
  }
}
