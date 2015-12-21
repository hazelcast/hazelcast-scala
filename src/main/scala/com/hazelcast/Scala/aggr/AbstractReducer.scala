package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

trait AbstractReducer[T] extends Aggregator[T, T, T] {
  def init: T
  def reduce(x: T, y: T): T
  final def remoteInit = init
  final def remoteFold(a: T, t: T): T = reduce(a, t)
  final def remoteCombine(x: T, y: T): T = reduce(x, y)
  final def remoteFinalize(t: T): T = t
  final def localCombine(x: T, y: T): T = reduce(x, y)
}
