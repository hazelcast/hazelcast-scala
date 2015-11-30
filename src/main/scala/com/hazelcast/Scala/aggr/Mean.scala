package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregation

class Mean[N: Numeric] extends Aggr2[N, Option[N], N, N, N, Int, Int, Int](new Sum[N], Count) {
  private[this] val n = implicitly[Numeric[N]]
  private[this] val divide = {
    n match {
      case f: Fractional[N] => f.div _
      case i: Integral[N] => i.quot _
      case _ => (a: N, b: N) => n.fromInt(math.round(n.toFloat(a) / n.toFloat(b)))
    }
  }
  def localFinalize(sumCount: (N, Int)): Option[N] = sumCount match {
    case (_, 0) => None
    case (sum, count) => Some(divide(sum, n.fromInt(count)))
  }
}
