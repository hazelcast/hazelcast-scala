package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object Distinct {
  type Q[T] = Set[T]
  type W[T] = Q[T]
  type R[T] = W[T]

  def apply[T]() = new Distinct[T]

  class Distinct[T] extends Aggregator[Q[T], T, W[T], R[T]] {
    type Q = Distinct.Q[T]
    type W = Q
    type R = W
    def remoteInit = Set.empty[T]
    def remoteFold(set: Q, t: T) = set + t
    def remoteCombine(x: Q, y: Q) = x ++ y
    def remoteFinalize(set: W) = set
    def localCombine(x: W, y: W) = x ++ y
    def localFinalize(set: W) = set
  }
}
