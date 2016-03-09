package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

object SumCount {
  def apply[N: Numeric]: Aggregator[N, (N, Int)] = implicitly[Numeric[N]].zero match {
    case _: Double => new DoubleSumCount().asInstanceOf[Aggregator[N, (N, Int)]]
    case _: Long => new LongSumCount().asInstanceOf[Aggregator[N, (N, Int)]]
    case _: Float => new FloatSumCount().asInstanceOf[Aggregator[N, (N, Int)]]
    case _: Int => new IntSumCount().asInstanceOf[Aggregator[N, (N, Int)]]
    case _ => new NumericSumCount[N]
  }
  final class NumericSumCount[N: Numeric] extends Aggregator[N, (N, Int)] {
    class Mutable(var sum: N, var count: Int)
    type Q = Mutable
    type W = (N, Int)
    @inline def num = implicitly[Numeric[N]]
    def remoteInit: Q = new Mutable(num.zero, 0)
    def remoteFold(q: Q, n: N): Q = {
      q.sum = num.plus(q.sum, n)
      q.count += 1
      q
    }
    def remoteCombine(x: Q, y: Q): Q = new Mutable(num.plus(x.sum, y.sum), x.count + y.count)
    def remoteFinalize(q: Q): W = q.sum -> q.count
    def localCombine(x: W, y: W): W = num.plus(x._1, y._1) -> (x._2 + y._2)
    def localFinalize(w: W): (N, Int) = w
  }
  final class LongSumCount extends Aggregator[Long, (Long, Int)] {
    class Mutable(var sum: Long, var count: Int)
    type Q = Mutable
    type W = (Long, Int)
    def remoteInit: Q = new Mutable(0L, 0)
    def remoteFold(q: Q, n: Long): Q = {
      q.sum += n
      q.count += 1
      q
    }
    def remoteCombine(x: Q, y: Q): Q = new Mutable(x.sum + y.sum, x.count + y.count)
    def remoteFinalize(q: Q): W = q.sum -> q.count
    def localCombine(x: W, y: W): W = (x._1 + y._1) -> (x._2 + y._2)
    def localFinalize(w: W): (Long, Int) = w
  }
  final class DoubleSumCount extends Aggregator[Double, (Double, Int)] {
    class Mutable(var sum: Double, var count: Int)
    type Q = Mutable
    type W = (Double, Int)
    def remoteInit: Q = new Mutable(0d, 0)
    def remoteFold(q: Q, n: Double): Q = {
      q.sum += n
      q.count += 1
      q
    }
    def remoteCombine(x: Q, y: Q): Q = new Mutable(x.sum + y.sum, x.count + y.count)
    def remoteFinalize(q: Q): W = q.sum -> q.count
    def localCombine(x: W, y: W): W = (x._1 + y._1) -> (x._2 + y._2)
    def localFinalize(w: W): (Double, Int) = w
  }
  final class FloatSumCount extends Aggregator[Float, (Float, Int)] {
    class Mutable(var sum: Float, var count: Int)
    type Q = Mutable
    type W = (Float, Int)
    def remoteInit: Q = new Mutable(0f, 0)
    def remoteFold(q: Q, n: Float): Q = {
      q.sum += n
      q.count += 1
      q
    }
    def remoteCombine(x: Q, y: Q): Q = new Mutable(x.sum + y.sum, x.count + y.count)
    def remoteFinalize(q: Q): W = q.sum -> q.count
    def localCombine(x: W, y: W): W = (x._1 + y._1) -> (x._2 + y._2)
    def localFinalize(w: W): (Float, Int) = w
  }
  final class IntSumCount extends Aggregator[Int, (Int, Int)] {
    class Mutable(var sum: Int, var count: Int)
    type Q = Mutable
    type W = (Int, Int)
    def remoteInit: Q = new Mutable(0, 0)
    def remoteFold(q: Q, n: Int): Q = {
      q.sum += n
      q.count += 1
      q
    }
    def remoteCombine(x: Q, y: Q): Q = new Mutable(x.sum + y.sum, x.count + y.count)
    def remoteFinalize(q: Q): W = q.sum -> q.count
    def localCombine(x: W, y: W): W = (x._1 + y._1) -> (x._2 + y._2)
    def localFinalize(w: W): (Int, Int) = w
  }
}
