package com.hazelcast.Scala.aggr

final class SumCount[N: Numeric]
    extends MultiFinalizeAdapter[N, (N, Int)](Sum[N](), Count) {

  protected final def num = implicitly[Numeric[N]]

  def localFinalize(sumCount: W): (N, Int) = {
    val a0 = aggrs(0)
    val sum = sumCount(0).asInstanceOf[a0.W]
    val a1 = aggrs(1)
    val count = sumCount(1).asInstanceOf[a1.W]
    a0.localFinalize(sum).asInstanceOf[N] -> a1.localFinalize(count).asInstanceOf[Int]
  }
}
