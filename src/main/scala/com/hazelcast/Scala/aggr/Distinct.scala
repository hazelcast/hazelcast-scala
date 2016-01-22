package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object Distinct {

  def apply[T](): Aggregator[T, Set[T]] = new Distinct[T]

  class Distinct[T] extends Aggregator[T, Set[T]] {
    type Q = Set[T]
    type W = Set[T]
    def remoteInit = Set.empty[T]
    def remoteFold(set: Q, t: T) = set + t
    def remoteCombine(x: Q, y: Q) = x ++ y
    def remoteFinalize(set: W) = set
    def localCombine(x: W, y: W) = x ++ y
    def localFinalize(set: W) = set
  }
}
