package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Freq
import com.hazelcast.Scala.Aggregator
import collection.JavaConverters._

object Distribution {
  type Q[T] = java.util.HashMap[T, Integer]
  type W[T] = Q[T]
  type R[T] = collection.mutable.Map[T, Freq]

  def apply[T]() = new Distribution[T]

  class Distribution[T] extends Aggregator[Q[T], T, W[T], R[T]] {
    type Q = Distribution.Q[T]
    type R = Distribution.R[T]

    def remoteInit = new Q(128)
    def remoteCombine(x: Q, y: Q): Q = combine(x, y)
    def remoteFold(map: Q, t: T) = map.put(t, 1) match {
      case null => map
      case freq =>
        map.put(t, freq + 1)
        map
    }

    def remoteFinalize(map: Q) = map

    private def combine(jx: Q, jy: Q) = {
      val sy = jy.asScala
      val (intersection, yOnly) = sy.partition(e => jx.containsKey(e._1))
      val combined = intersection.foldLeft(jx) {
        case (map, entry) =>
          map.put(entry._1, map.get(entry._1) + entry._2)
          map
      }
      combined.putAll(yOnly.asJava)
      combined
    }

    def localCombine(jx: Q, jy: Q) = combine(jx, jy)
    def localFinalize(dist: Q) = dist.asScala.asInstanceOf[R]
  }

}
