package com.hazelcast.Scala

package aggr {

  abstract class FinalizeAdapter[T, R, R1](
    val a: Aggregator[T, R1])
      extends Aggregator[T, R] {
    type Q = a.Q
    type W = a.W
    def remoteInit = a.remoteInit
    def remoteFold(q: Q, t: T): Q = a.remoteFold(q, t)
    def remoteCombine(x: Q, y: Q): Q = a.remoteCombine(x, y)
    def remoteFinalize(q: Q) = a.remoteFinalize(q)
    def localCombine(x: W, y: W): W = a.localCombine(x, y)
  }

  abstract class MultiFinalizeAdapter[T, R](protected final val aggrs: Aggregator[T, _]*)
      extends Aggregator[T, R] {
    type Q = Array[Any]
    type W = Array[Any]
    def remoteInit: Q = {
      val arr = new Array[Any](aggrs.length)
      var i = 0
      while (i < arr.length) {
        arr(i) = aggrs(i).remoteInit
        i += 1
      }
      arr
    }
    def remoteFold(q: Q, t: T): Q = {
      var i = 0
      while (i < q.length) {
        val aggr = aggrs(i)
        q(i) = aggr.remoteFold(q(i).asInstanceOf[aggr.Q], t)
        i += 1
      }
      q
    }
    def remoteCombine(x: Q, y: Q): Q = {
      var i = 0
      while (i < x.length) {
        val aggr = aggrs(i)
        x(i) = aggr.remoteCombine(x(i).asInstanceOf[aggr.Q], y(i).asInstanceOf[aggr.Q])
        i += 1
      }
      x
    }
    def remoteFinalize(q: Q) = {
      var i = 0
      while (i < q.length) {
        val aggr = aggrs(i)
        q(i) = aggr.remoteFinalize(q(i).asInstanceOf[aggr.Q])
        i += 1
      }
      q
    }
    def localCombine(x: W, y: W): W = {
      var i = 0
      while (i < x.length) {
        val aggr = aggrs(i)
        x(i) = aggr.localCombine(x(i).asInstanceOf[aggr.W], y(i).asInstanceOf[aggr.W])
        i += 1
      }
      x
    }
  }
}

package object aggr {

  val PartialFunctionUnit: PartialFunction[Unit, Unit] = { case u => u }

}