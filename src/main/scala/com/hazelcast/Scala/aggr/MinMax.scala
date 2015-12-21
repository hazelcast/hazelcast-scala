package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator

class Min[O: Ordering] extends AbstractReducer[O, Option[O]] {
  @inline private def o = implicitly[Ordering[O]]
  def init = null.asInstanceOf[O]
  def reduce(x: O, y: O): O =
    if (x == null) y
    else if (y == null) x
    else o.min(x, y)
  def localFinalize(o: O) = Option(o)
}
class Max[O: Ordering] extends AbstractReducer[O, Option[O]] {
  @inline private def o = implicitly[Ordering[O]]
  def init = null.asInstanceOf[O]
  def reduce(x: O, y: O): O =
    if (x == null) y
    else if (y == null) x
    else o.max(x, y)
  def localFinalize(o: O) = Option(o)
}

class MinMax[O: Ordering] extends Aggregator[(O, O), O, (O, O), Option[(O, O)]] {
  @inline private def o = implicitly[Ordering[O]]
  def remoteInit = null.asInstanceOf[(O, O)]
  def remoteFold(minMax: (O, O), value: O): (O, O) = if (minMax == null) (value, value) else (o.min(minMax._1, value), o.max(minMax._2, value))
  def remoteCombine(x: (O, O), y: (O, O)): (O, O) = reduce(x, y)
  def remoteFinalize(minMax: (O, O)): (O, O) = minMax
  private def reduce(x: (O, O), y: (O, O)): (O, O) =
    if (x == null) y
    else if (y == null) x
    else o.min(x._1, y._1) -> o.max(x._2, y._2)
  def localCombine(x: (O, O), y: (O, O)): (O, O) = reduce(x, y)
  def localFinalize(o: (O, O)) = Option(o)
}
