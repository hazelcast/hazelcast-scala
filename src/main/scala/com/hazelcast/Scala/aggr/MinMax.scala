package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

class Min[E, O: Ordering](m: E => O) extends AbstractReducer[E, Option[E]] {
  def init = null.asInstanceOf[E]
  def reduce(x: E, y: E): E =
    if (x == null) y
    else if (y == null) x
    else MinMax.min(x, y, m)
  def localFinalize(e: E) = Option(e)
}
class Max[E, O: Ordering](m: E => O) extends AbstractReducer[E, Option[E]] {
  def init = null.asInstanceOf[E]
  def reduce(x: E, y: E): E =
    if (x == null) y
    else if (y == null) x
    else MinMax.max(x, y, m)
  def localFinalize(e: E) = Option(e)
}

private object MinMax {
  def min[E, O: Ordering](x: E, y: E, m: E => O): E =
    if (implicitly[Ordering[O]].lteq(m(x), m(y))) x
    else y
  def max[E, O: Ordering](x: E, y: E, m: E => O): E =
    if (implicitly[Ordering[O]].gteq(m(x), m(y))) x
    else y
}
class MinMax[E, O: Ordering](m: E => O) extends Aggregator[E, Option[(E, E)]] {
  type Q = (E, E)
  type W = Q
  def remoteInit = null
  def remoteFold(minMax: Q, value: E): Q =
    if (minMax == null) (value, value)
    else MinMax.min(minMax._1, value, m) -> MinMax.max(minMax._2, value, m)
  def remoteCombine(x: W, y: W): W = reduce(x, y)
  def remoteFinalize(minMax: W): W = minMax
  private def reduce(x: W, y: W): W =
    if (x == null) y
    else if (y == null) x
    else MinMax.min(x._1, y._1, m) -> MinMax.max(x._2, y._2, m)
  def localCombine(x: W, y: W): W = reduce(x, y)
  def localFinalize(mm: W) = Option(mm)
}
