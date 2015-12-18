package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator.SimpleReducer

class Sum[N: Numeric] extends SimpleReducer[N] {
  @inline private def n = implicitly[Numeric[N]]
  def init = n.zero
  def reduce(sum: N, value: N) = n.plus(sum, value)
}
