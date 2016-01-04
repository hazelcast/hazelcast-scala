package com.hazelcast.Scala

package aggr {

  abstract class FinalizeAdapter[T, Q1, W1, R1](
    protected val a1: Aggregator[Q1, T, W1] { type R = R1 })
      extends Aggregator[Q1, T, W1] {
    type Q = Q1
    def remoteInit = a1.remoteInit
    def remoteFold(q: Q, t: T): Q = a1.remoteFold(q, t)
    def remoteCombine(x: Q, y: Q): Q = a1.remoteCombine(x, y)
    def remoteFinalize(q: Q) = a1.remoteFinalize(q)
    def localCombine(x: W1, y: W1): W1 = a1.localCombine(x, y)
  }

  abstract class FinalizeAdapter2[T, Q1, W1, R1, Q2, W2, R2](
    val a1: Aggregator[Q1, T, W1] { type R = R1 },
    val a2: Aggregator[Q2, T, W2] { type R = R2 })
      extends Aggregator[(Q1, Q2), T, (W1, W2)] {
    type Q = (Q1, Q2)
    def remoteInit: Q = (a1.remoteInit, a2.remoteInit)
    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t))
    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2))
    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2))
    def localCombine(x: (W1, W2), y: (W1, W2)): (W1, W2) = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2))
  }
  abstract class FinalizeAdapter3[T, Q1, W1, R1, Q2, W2, R2, Q3, W3, R3](
    val a1: Aggregator[Q1, T, W1] { type R = R1 },
    val a2: Aggregator[Q2, T, W2] { type R = R2 },
    val a3: Aggregator[Q3, T, W3] { type R = R3 })
      extends Aggregator[(Q1, Q2, Q3), T, (W1, W2, W3)] {
    type Q = (Q1, Q2, Q3)
    def remoteInit: Q = (a1.remoteInit, a2.remoteInit, a3.remoteInit)
    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t), a3.remoteFold(q._3, t))
    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2), a3.remoteCombine(x._3, y._3))
    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2), a3.remoteFinalize(q._3))
    def localCombine(x: (W1, W2, W3), y: (W1, W2, W3)): (W1, W2, W3) = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2), a3.localCombine(x._3, y._3))
  }
  abstract class FinalizeAdapter4[T, Q1, W1, R1, Q2, W2, R2, Q3, W3, R3, Q4, W4, R4](
    val a1: Aggregator[Q1, T, W1] { type R = R1 },
    val a2: Aggregator[Q2, T, W2] { type R = R2 },
    val a3: Aggregator[Q3, T, W3] { type R = R3 },
    val a4: Aggregator[Q4, T, W4] { type R = R4 })
      extends Aggregator[(Q1, Q2, Q3, Q4), T, (W1, W2, W3, W4)] {
    type Q = (Q1, Q2, Q3, Q4)
    def remoteInit: Q = (a1.remoteInit, a2.remoteInit, a3.remoteInit, a4.remoteInit)
    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t), a3.remoteFold(q._3, t), a4.remoteFold(q._4, t))
    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2), a3.remoteCombine(x._3, y._3), a4.remoteCombine(x._4, y._4))
    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2), a3.remoteFinalize(q._3), a4.remoteFinalize(q._4))
    def localCombine(x: (W1, W2, W3, W4), y: (W1, W2, W3, W4)): (W1, W2, W3, W4) = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2), a3.localCombine(x._3, y._3), a4.localCombine(x._4, y._4))
  }

}
