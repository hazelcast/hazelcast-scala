package com.hazelcast.Scala.aggr

class Mean[N: Numeric]
    extends FinalizeAdapter2[N, N, N, N, Int, Int, Int](new Sum[N], Count)
    with DivisionSupport[N] {
  type R = Option[N]
  protected final def num = implicitly[Numeric[N]]
  def localFinalize(sumCount: (N, Int)): Option[N] = sumCount match {
    case (_, 0) => None
    case (sum, count) =>
      val divide = divOp
      Some(divide(sum, num.fromInt(count)))
  }
}
