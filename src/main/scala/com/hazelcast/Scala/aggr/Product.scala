package com.hazelcast.Scala.aggr

final class Product[N: Numeric] extends SimpleReducer[N] {
  @inline private def n = implicitly[Numeric[N]]
  def init = n.one
  def reduce(product: N, value: N) = n.times(product, value)
}
