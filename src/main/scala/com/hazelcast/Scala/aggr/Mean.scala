package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregation

class Mean[N: Numeric]
    extends FinalizeAdapter2[N, Option[N], N, N, N, Int, Int, Int](new Sum[N], Count)
    with DivisionSupport[N] {
  protected final def num = implicitly[Numeric[N]]
  def localFinalize(sumCount: (N, Int)): Option[N] = sumCount match {
    case (_, 0) => None
    case (sum, count) =>
      val divide = divOp
      Some(divide(sum, num.fromInt(count)))
  }
}
