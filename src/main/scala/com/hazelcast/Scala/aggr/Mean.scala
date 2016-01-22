package com.hazelcast.Scala.aggr

final class Mean[N: Numeric]
    extends FinalizeAdapter[N, Option[N], (N, Int)](new SumCount[N])
    with DivisionSupport[N] {

  protected final def num = implicitly[Numeric[N]]

  def localFinalize(sumCount: W): Option[N] = {
    a.localFinalize(sumCount) match {
      case (_, 0) => None
      case (sum, count: Int) =>
        val divide = divOp
        Some(divide(sum, num.fromInt(count)))
    }
  }
}
