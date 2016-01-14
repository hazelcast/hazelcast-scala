package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object Product {

  def apply[N: Numeric](): Aggregator[N, N] = implicitly[Numeric[N]].zero match {
    case _: Double => new DoubleProduct[Double].asInstanceOf[Aggregator[N, N]]
    case _: Long => new LongProduct[Long].asInstanceOf[Aggregator[N, N]]
    case _: Float => new FloatProduct[Float].asInstanceOf[Aggregator[N, N]]
    case _: Int => new IntProduct[Int].asInstanceOf[Aggregator[N, N]]
    case _ => new NumericProduct[N]
  }

  private final class NumericProduct[N: Numeric] extends SimpleReducer[N] {
    @inline private def n = implicitly[Numeric[N]]
    def init = n.one
    def reduce(product: N, value: N) = n.times(product, value)
  }
  private final class IntProduct[N <: Int] extends IntReducer[Int, Int] {
    def reduce(x: Int, y: Int): Int = x * y
  }
  private final class LongProduct[N <: Long] extends LongReducer[Long, Long] {
    def reduce(x: Long, y: Long): Long = x * y
  }
  private final class FloatProduct[N <: Float] extends FloatReducer[Float, Float] {
    def reduce(x: Float, y: Float): Float = x * y
  }
  private final class DoubleProduct[N <: Double] extends DoubleReducer[Double, Double] {
    def reduce(x: Double, y: Double): Double = x * y
  }

}
