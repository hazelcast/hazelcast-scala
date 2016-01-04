package com.hazelcast.Scala.aggr

final class SumCount[N: Numeric]
    extends FinalizeAdapter2[N, N, N, N, Int, Int, Int](new Sum[N], Count) {
  type R = (N, Int)
  def localFinalize(sumCount: (N, Int)) = sumCount
}
