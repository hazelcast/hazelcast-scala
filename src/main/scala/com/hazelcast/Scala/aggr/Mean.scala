package com.hazelcast.Scala.aggr

final class Mean[N: Numeric]
    extends MultiFinalizeAdapter[N, Option[N]](Sum[N](), Count)
    with DivisionSupport[N] {

  protected final def num = implicitly[Numeric[N]]

  def localFinalize(sumCount: W): Option[N] = {
    val a0 = aggrs(0)
    val sum = sumCount(0).asInstanceOf[a0.W]
    val a1 = aggrs(1)
    val count = sumCount(1).asInstanceOf[a1.W]
    a0.localFinalize(sum) -> a1.localFinalize(count) match {
      case (_, 0) => None
      case (sum: N, count: Int) =>
        val divide = divOp
        Some(divide(sum, num.fromInt(count)))
    }
  }
}
