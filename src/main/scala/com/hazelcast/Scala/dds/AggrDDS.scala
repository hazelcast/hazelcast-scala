package com.hazelcast.Scala.dds

import scala.concurrent._
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import scala.reflect.ClassTag
import collection.{ Map => aMap }
import collection.immutable._
import collection.{ Seq, IndexedSeq }
import com.hazelcast.core.IExecutorService

private[dds] object AggrDDS {
  def sortByFreq[E](dist: aMap[E, Freq], top: Int): SortedMap[Freq, Set[E]] = {
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
  def mode[E](distribution: aMap[E, Freq]): Set[E] = {
    sortByFreq(distribution, 1).headOption match {
      case None => Set.empty
      case Some((_, mode)) =>
        if (mode.size == distribution.size) Set.empty
        else mode
    }
  }
}

trait AggrDDS[E] {
  def submit[R](aggregator: Aggregator[E, R], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[R]
  def fetch()(implicit classTag: ClassTag[E], ec: ExecutionContext): Future[IndexedSeq[E]] = this submit aggr.Fetch[E]()
  def distinct()(implicit ec: ExecutionContext): Future[Set[E]] = this submit aggr.Distinct()
  def distribution()(implicit ec: ExecutionContext): Future[aMap[E, Freq]] = this submit aggr.Distribution()
  def count()(implicit ec: ExecutionContext): Future[Int] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[Set[E]] = distribution().map(AggrDDS.mode)
  def frequency(top: Int = -1)(implicit ec: ExecutionContext): Future[SortedMap[Freq, Set[E]]] = {
    if (top == 0) Future successful SortedMap.empty
    else distribution().map(AggrDDS.sortByFreq(_, top))
  }
}

trait AggrGroupDDS[G, E] {
  def submitGrouped[AR, GR](aggr: Aggregator.Grouped[G, E, AR, GR], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[aMap[G, GR]]

  final def submit[R](aggr: Aggregator[E, R], es: IExecutorService = null)(implicit ec: ExecutionContext): Future[aMap[G, R]] =
    submitGrouped(Aggregator.groupAll(aggr), es)
  //  def fetch()(implicit classTag: ClassTag[E], ec: ExecutionContext) = this submit aggr.Fetch[E]()
  def distinct()(implicit ec: ExecutionContext): Future[aMap[G, Set[E]]] = submit(aggr.Distinct[E]())
  def distribution()(implicit ec: ExecutionContext): Future[aMap[G, aMap[E, Freq]]] = submit(aggr.Distribution[E]())
  def count()(implicit ec: ExecutionContext): Future[aMap[G, Int]] = submit(aggr.Count)
  def mode()(implicit ec: ExecutionContext): Future[aMap[G, Set[E]]] = distribution() map (_.mapValues(AggrDDS.mode))
  def frequency(top: Int = -1)(implicit ec: ExecutionContext): Future[aMap[G, SortedMap[Freq, Set[E]]]] = {
    if (top == 0) Future successful aMap.empty
    else distribution().map(_.mapValues(AggrDDS.sortByFreq(_, top)))
  }
}
