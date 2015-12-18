package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

final class Count(skip: Int, limit: Option[Int]) extends Aggregator[Int, Any, Int, Int] {
  def remoteInit = 0
  def remoteFold(count: Int, any: Any) = count + 1
  def remoteCombine(x: Int, y: Int) = x + y
  def remoteFinalize(count: Int) = count
  def localCombine(x: Int, y: Int) = x + y
  def localFinalize(count: Int) = {
    val limit = this.limit getOrElse Int.MaxValue
    (count - skip).max(0) min limit
  }
}
