package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object Variance {

  type Acc[N] = (Int, N, N)

  def apply[N: Numeric] = new Variance

  class Variance[N: Numeric]
      extends Aggregator[Acc[N], N, Acc[N], Option[N]]
      with DivisionSupport[N] {

    protected final def num = implicitly[Numeric[N]]
    private[this] val div = divOp

    type Q = Acc[N]
    type T = N
    type W = Q
    type R = N

    def remoteInit: Q = (0, num.zero, num.zero)
    def remoteFold(q: Q, x: T): Q = q match {
      case (0, _, s) => (1, x, s)
      case (count, m, s) =>
        val nCount = num.fromInt(count + 1)
        val delta = num.minus(x, m)
        val M = num.plus(m, div(delta, nCount))
        val newDelta = num.minus(x, M)
        val S = num.plus(s, num.times(delta, newDelta))
        (count + 1, M, S)
    }

    private def combine(x: Q, y: Q): Q = {
      val (xCount, xM, xS) = x
      val (yCount, yM, yS) = y
      if (xCount + yCount == 0) x
      else {
        val delta = num.minus(yM, xM)
        val xC = num.fromInt(xCount)
        val yC = num.fromInt(yCount)
        val count = num.fromInt(xCount + yCount)
        val M = num.plus(xM, num.times(yC, div(delta, count)))
        val q = num.times(num.times(xC, yC), div(num.times(delta, delta), count))
        val S = num.plus(q, num.plus(xS, yS))
        (xCount + yCount, M, S)
      }
    }

    def remoteCombine(x: Q, y: Q): Q = combine(x, y)
    def remoteFinalize(q: Q): W = q
    def localCombine(x: W, y: W): W = combine(x, y)
    def localFinalize(w: W): Option[N] = w match {
      case (0, _, _) => None
      case (1, _, _) => Some(num.zero)
      case (count, _, s) => Some(div(s, num.fromInt(count - 1)))
    }
  }

}
