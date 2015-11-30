package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregation.Simple

class Sum[N: Numeric] extends Simple[N] {
  @inline private def n = implicitly[Numeric[N]]
  def init = n.zero
  def reduce(sum: N, value: N) = n.plus(sum, value)
}
