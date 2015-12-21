package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object Count extends Aggregator[Int, Any, Int] {
  type R = Int
  final def remoteInit = 0
  final def remoteFold(count: Int, any: Any) = count + 1
  final def remoteCombine(x: Int, y: Int) = x + y
  final def remoteFinalize(count: Int) = count
  final def localCombine(x: Int, y: Int) = x + y
  final def localFinalize(count: Int) = count
}
