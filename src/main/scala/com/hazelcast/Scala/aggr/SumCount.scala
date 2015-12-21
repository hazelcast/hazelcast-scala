package com.hazelcast.Scala.aggr

final class SumCount[N: Numeric]
    extends FinalizeAdapter2[N, (N, Int), N, N, N, Int, Int, Int](new Sum[N], Count) {
  def localFinalize(sumCount: (N, Int)) = sumCount
}
