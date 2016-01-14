package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator
import scala.runtime.IntRef

object Count extends Aggregator[Any, Int] {
  type Q = IntRef
  type W = IntRef
  private def combine(x: IntRef, y: IntRef): IntRef = { x.elem += y.elem; x }
  final def remoteInit = new IntRef(0)
  final def remoteFold(count: IntRef, any: Any) = { count.elem += 1; count }
  final def remoteCombine(x: IntRef, y: IntRef) = combine(x, y)
  final def remoteFinalize(count: IntRef) = count
  final def localCombine(x: IntRef, y: IntRef) = combine(x, y)
  final def localFinalize(count: IntRef) = count.elem
}
