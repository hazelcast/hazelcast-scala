package com.hazelcast.Scala

package aggr {

  abstract class Aggr2[T, R, Q1, W1, R1, Q2, W2, R2](
    val a1: Aggregation[Q1, T, W1, R1],
    val a2: Aggregation[Q2, T, W2, R2])
      extends Aggregation[(Q1, Q2), T, (W1, W2), R] {
    type Q = (Q1, Q2)
    def remoteInit = (a1.remoteInit, a2.remoteInit)
    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t))
    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2))
    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2))
    def localCombine(x: (W1, W2), y: (W1, W2)): (W1, W2) = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2))
  }
  abstract class Aggr3[T, R, Q1, W1, R1, Q2, W2, R2, Q3, W3, R3](
    val a1: Aggregation[Q1, T, W1, R1],
    val a2: Aggregation[Q2, T, W2, R2],
    val a3: Aggregation[Q3, T, W3, R3])
      extends Aggregation[(Q1, Q2, Q3), T, (W1, W2, W3), R] {
    type Q = (Q1, Q2, Q3)
    def remoteInit = (a1.remoteInit, a2.remoteInit, a3.remoteInit)
    def remoteFold(q: Q, t: T): Q = (a1.remoteFold(q._1, t), a2.remoteFold(q._2, t), a3.remoteFold(q._3, t))
    def remoteCombine(x: Q, y: Q): Q = (a1.remoteCombine(x._1, y._1), a2.remoteCombine(x._2, y._2), a3.remoteCombine(x._3, y._3))
    def remoteFinalize(q: Q) = (a1.remoteFinalize(q._1), a2.remoteFinalize(q._2), a3.remoteFinalize(q._3))
    def localCombine(x: (W1, W2), y: (W1, W2)): (W1, W2) = (a1.localCombine(x._1, y._1), a2.localCombine(x._2, y._2))
  }

}
