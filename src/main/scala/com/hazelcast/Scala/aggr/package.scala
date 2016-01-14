package com.hazelcast.Scala

import scala.reflect.{ ClassTag, classTag }

package aggr {

  abstract class FinalizeAdapter[T, R, R1](
    val a1: Aggregator[T, R1])
      extends Aggregator[T, R] {
    type Q = a1.Q
    type W = a1.W
    def remoteInit = a1.remoteInit
    def remoteFold(q: Q, t: T): Q = a1.remoteFold(q, t)
    def remoteCombine(x: Q, y: Q): Q = a1.remoteCombine(x, y)
    def remoteFinalize(q: Q) = a1.remoteFinalize(q)
    def localCombine(x: W, y: W): W = a1.localCombine(x, y)
  }

//  abstract class FinalizeAdapter2[T, R, R1, R2](
//    val a1: Aggregator[T, R1],
//    val a2: Aggregator[T, R2])
//      extends Aggregator[T, R] {
//    type Q = (a1.Q, a2.Q)
//    type W = (a1.W, a2.W)
//    def remoteInit: Q = (a1.remoteInit, a2.remoteInit)
//    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t))
//    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2))
//    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2))
//    def localCombine(x: W, y: W): W = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2))
//  }
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
  //  abstract class FinalizeAdapter3[T, Q1, W1, R1, Q2, W2, R2, Q3, W3, R3](
  //    val a1: Aggregator[Q1, T, W1] { type R = R1 },
  //    val a2: Aggregator[Q2, T, W2] { type R = R2 },
  //    val a3: Aggregator[Q3, T, W3] { type R = R3 })
  //      extends Aggregator[(Q1, Q2, Q3), T, (W1, W2, W3)] {
  //    type Q = (Q1, Q2, Q3)
  //    def remoteInit: Q = (a1.remoteInit, a2.remoteInit, a3.remoteInit)
  //    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t), a3.remoteFold(q._3, t))
  //    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2), a3.remoteCombine(x._3, y._3))
  //    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2), a3.remoteFinalize(q._3))
  //    def localCombine(x: (W1, W2, W3), y: (W1, W2, W3)): (W1, W2, W3) = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2), a3.localCombine(x._3, y._3))
  //  }
  //  abstract class FinalizeAdapter4[T, Q1, W1, R1, Q2, W2, R2, Q3, W3, R3, Q4, W4, R4](
  //    val a1: Aggregator[Q1, T, W1] { type R = R1 },
  //    val a2: Aggregator[Q2, T, W2] { type R = R2 },
  //    val a3: Aggregator[Q3, T, W3] { type R = R3 },
  //    val a4: Aggregator[Q4, T, W4] { type R = R4 })
  //      extends Aggregator[(Q1, Q2, Q3, Q4), T, (W1, W2, W3, W4)] {
  //    type Q = (Q1, Q2, Q3, Q4)
  //    def remoteInit: Q = (a1.remoteInit, a2.remoteInit, a3.remoteInit, a4.remoteInit)
  //    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t), a3.remoteFold(q._3, t), a4.remoteFold(q._4, t))
  //    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2), a3.remoteCombine(x._3, y._3), a4.remoteCombine(x._4, y._4))
  //    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2), a3.remoteFinalize(q._3), a4.remoteFinalize(q._4))
  //    def localCombine(x: (W1, W2, W3, W4), y: (W1, W2, W3, W4)): (W1, W2, W3, W4) = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2), a3.localCombine(x._3, y._3), a4.localCombine(x._4, y._4))
  //  }
  //
}
