package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object Sum {

  def apply[N: Numeric](): Aggregator[N, N] = implicitly[Numeric[N]].zero match {
    case _: Double => new DoubleSum[Double].asInstanceOf[Aggregator[N, N]]
    case _: Long => new LongSum[Long].asInstanceOf[Aggregator[N, N]]
    case _: Float => new FloatSum[Float].asInstanceOf[Aggregator[N, N]]
    case _: Int => new IntSum[Int].asInstanceOf[Aggregator[N, N]]
    case _ => new NumericSum[N]
  }

  private final class NumericSum[N: Numeric] extends SimpleReducer[N] {
    @inline private def n = implicitly[Numeric[N]]
    def init = n.zero
    def reduce(sum: N, value: N) = n.plus(sum, value)
  }
  private final class IntSum[N <: Int] extends IntReducer[Int, Int] {
    def reduce(x: Int, y: Int): Int = x + y
  }
  private final class LongSum[N <: Long] extends LongReducer[Long, Long] {
    def reduce(x: Long, y: Long): Long = x + y
  }
  private final class FloatSum[N <: Float] extends FloatReducer[Float, Float] {
    def reduce(x: Float, y: Float): Float = x + y
  }
  private final class DoubleSum[N <: Double] extends DoubleReducer[Double, Double] {
    def reduce(x: Double, y: Double): Double = x + y
  }

}
