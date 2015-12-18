package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator
import scala.collection.immutable.{ HashSet, TreeSet }

object Distinct {
  type Q[T] = Set[T]
  type W[T] = Q[T]
  type R[T] = W[T]

  def apply[T](ordering: Option[Ordering[T]], skip: Int, limit: Option[Int]) = new Distinct[T](ordering, skip, limit)

  class Distinct[T](ordering: Option[Ordering[T]], skip: Int, limit: Option[Int]) extends Aggregator[Q[T], T, W[T], R[T]] {
    type Q = Distinct.Q[T]
    type W = Q
    type R = W
    def remoteInit = ordering match {
      case None => new HashSet[T]
      case Some(ordering) => new TreeSet[T]()(ordering)
    }
    private def calcMaxSize = {
      this.limit.getOrElse(Int.MaxValue - skip) + skip
    }
    private[this] val limitSizeOnFold = ordering.isEmpty // Only check size on HashMap O(1), not TreeMap O(n)
    private[this] val foldMaxSize = if (limitSizeOnFold) calcMaxSize else -1
    def remoteFold(set: Q, t: T) = if (limitSizeOnFold && set.size == foldMaxSize) set else set + t
    def remoteCombine(x: Q, y: Q) =
      if (limitSizeOnFold && x.size == foldMaxSize) x
      else if (limitSizeOnFold && y.size == foldMaxSize) y
      else x ++ y
    def remoteFinalize(set: W) = {
      val remoteMaxSize = calcMaxSize
      if (set.size > remoteMaxSize) set.take(remoteMaxSize) else set
    }
    def localCombine(x: W, y: W) = x ++ y
    def localFinalize(set: W) = {
      val skipped = if (skip > 0) set.drop(skip) else set
      limit.map(skipped.take) getOrElse skipped
    }
  }
}
