package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object Variance {

  def NoCorrection[N: Numeric]: Int => N = (n: Int) => implicitly[Numeric[N]].fromInt(n)

  def apply[N: Numeric](nCorrection: Int => N): Aggregator[N, Option[N]] =
    implicitly[Numeric[N]].zero match {
      case _: Double => new DoubleVariance[Double](nCorrection.asInstanceOf[Int => Double]).asInstanceOf[Aggregator[N, Option[N]]]
      case _ => new NumericVariance(nCorrection)
    }

  private class NumericVariance[N: Numeric] private[aggr] (nCorrection: Int => N)
      extends Aggregator[N, Option[N]]
      with DivisionSupport[N] {

    protected final def num = implicitly[Numeric[N]]
    private[this] val div = divOp

    type Q = (Int, N, N)
    type T = N
    type W = Q
    type R = Option[N]

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
    def localFinalize(w: W): R = {
      w match {
        case (0, _, _) => None
        case (1, _, _) => Some(num.zero)
        case (count, _, s) =>
          val corrected = nCorrection(count)
          val gt0 =
            if (num.gt(corrected, num.zero)) corrected
            else num.fromInt(count) // FIXME: How to handle small sample size?
          Some(div(s, gt0))
      }
    }
  }

  private class DblAcc(var count: Int = 0, var m: Double = 0, var s: Double = 0) extends Serializable
  private class DoubleVariance[N <: Double] private[aggr] (nCorrection: Int => N)
      extends Aggregator[N, Option[N]] {

    type Q = DblAcc
    type T = N
    type W = Q

    def remoteInit: Q = new Q
    def remoteFold(q: Q, x: T): Q = {
      q.count += 1
      if (q.count == 1) {
        q.m = x
      } else {
        val delta = x - q.m
        q.m += delta / q.count
        val newDelta = x - q.m
        q.s += delta * newDelta
      }
      q
    }
    private def combine(x: Q, y: Q): Q = {
      if (x.count + y.count == 0) x
      else {
        val delta = y.m - x.m
        val count = x.count + y.count
        x.m += y.count * (delta / count)
        val q = x.count * y.count * ((delta * delta) / count)
        x.s += q + y.s
        x.count = count
        x
      }
    }

    def remoteCombine(x: Q, y: Q): Q = combine(x, y)
    def remoteFinalize(q: Q): W = q
    def localCombine(x: W, y: W): W = combine(x, y)
    def localFinalize(w: W): Option[N] = {
      w.count match {
        case 0 => None
        case 1 => Some(0d.asInstanceOf[N])
        case _ =>
          val corrected = nCorrection(w.count).doubleValue
          val gt0 =
            if (corrected > 0d) corrected
            else w.count // FIXME: How to handle small sample size?
          Some((w.s / gt0).asInstanceOf[N])
      }
    }
  }

}
