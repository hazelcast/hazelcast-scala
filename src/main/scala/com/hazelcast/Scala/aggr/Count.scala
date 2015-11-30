package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregation

object Count extends Aggregation[Int, Any, Int, Int] {
  def remoteInit = 0
  def remoteFold(count: Int, any: Any) = count + 1
  def remoteCombine(x: Int, y: Int) = x + y
  def remoteFinalize(count: Int) = count
  def localCombine(x: Int, y: Int) = x + y
  def localFinalize(count: Int) = count
}
