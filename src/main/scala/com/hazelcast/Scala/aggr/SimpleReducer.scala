package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator
import scala.runtime.LongRef
import scala.runtime.IntRef
import scala.runtime.FloatRef
import scala.runtime.DoubleRef

trait SimpleReducer[T] extends AbstractReducer[T, T] {
  final def localFinalize(t: T): T = t
}

trait LongReducer[N <: Long, R] extends Aggregator[N, N] {
  type Q = LongRef
  type W = LongRef
  @inline private def combine(x: LongRef, y: LongRef): LongRef = {
    x.elem = reduce(x.elem, y.elem)
    x
  }
  final def remoteInit = LongRef.zero
  final def remoteFold(a: LongRef, t: Long): LongRef = { a.elem = reduce(a.elem, t); a }
  final def remoteCombine(x: LongRef, y: LongRef): LongRef = combine(x, y)
  final def remoteFinalize(t: LongRef): LongRef = t
  final def localCombine(x: LongRef, y: LongRef): LongRef = combine(x, y)
  final def localFinalize(lr: LongRef): N = lr.elem.asInstanceOf[N]

  protected def reduce(x: Long, y: Long): Long
}
trait IntReducer[N <: Int, R] extends Aggregator[N, N] {
  type Q = IntRef
  type W = IntRef

  @inline private def combine(x: IntRef, y: IntRef): IntRef = {
    x.elem = reduce(x.elem, y.elem)
    x
  }
  final def remoteInit = IntRef.zero
  final def remoteFold(a: IntRef, t: Int): IntRef = { a.elem = reduce(a.elem, t); a }
  final def remoteCombine(x: IntRef, y: IntRef): IntRef = combine(x, y)
  final def remoteFinalize(t: IntRef): IntRef = t
  final def localCombine(x: IntRef, y: IntRef): IntRef = combine(x, y)
  final def localFinalize(lr: IntRef): N = lr.elem.asInstanceOf[N]

  protected def reduce(x: Int, y: Int): Int

}

trait FloatReducer[N <: Float, R] extends Aggregator[N, N] {
  type Q = FloatRef
  type W = FloatRef
  @inline private def combine(x: FloatRef, y: FloatRef): FloatRef = {
    x.elem = reduce(x.elem, y.elem)
    x
  }
  final def remoteInit = FloatRef.zero
  final def remoteFold(a: FloatRef, t: Float): FloatRef = { a.elem = reduce(a.elem, t); a }
  final def remoteCombine(x: FloatRef, y: FloatRef): FloatRef = combine(x, y)
  final def remoteFinalize(t: FloatRef): FloatRef = t
  final def localCombine(x: FloatRef, y: FloatRef): FloatRef = combine(x, y)
  final def localFinalize(lr: FloatRef): N = lr.elem.asInstanceOf[N]

  protected def reduce(x: Float, y: Float): Float
}
trait DoubleReducer[N <: Double, R] extends Aggregator[N, N] {
  type Q = DoubleRef
  type W = DoubleRef
  @inline private def combine(x: DoubleRef, y: DoubleRef): DoubleRef = {
    x.elem = reduce(x.elem, y.elem)
    x
  }
  final def remoteInit = DoubleRef.zero
  final def remoteFold(a: DoubleRef, t: Double): DoubleRef = { a.elem = reduce(a.elem, t); a }
  final def remoteCombine(x: DoubleRef, y: DoubleRef): DoubleRef = combine(x, y)
  final def remoteFinalize(t: DoubleRef): DoubleRef = t
  final def localCombine(x: DoubleRef, y: DoubleRef): DoubleRef = combine(x, y)
  final def localFinalize(lr: DoubleRef): N = lr.elem.asInstanceOf[N]

  protected def reduce(x: Double, y: Double): Double
}
