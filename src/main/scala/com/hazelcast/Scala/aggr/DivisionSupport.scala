package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregation

trait DivisionSupport[N] {
  protected def num: Numeric[N]
  protected final def divOp = {
    num match {
      case f: Fractional[N] => f.div _
      case i: Integral[N] => i.quot _
      case n => (a: N, b: N) => n.fromInt(math.round(n.toFloat(a) / n.toFloat(b)))
    }
  }
}
