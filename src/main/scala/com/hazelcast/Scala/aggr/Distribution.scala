package com.hazelcast.Scala.aggr

import com.hazelcast.Scala._
import collection.JavaConverters._
import scala.runtime.IntRef

object Distribution {
  type Q[T] = java.util.HashMap[T, IntRef]
  type W[T] = collection.Map[T, Freq]
  type R[T] = W[T]

  def apply[T]() = new Distribution[T]

  class Distribution[T] extends Aggregator[T, R[T]] {
    type Q = Distribution.Q[T]
    type W = Distribution.W[T]

    def remoteInit = new Q(128)
    def remoteFold(map: Q, t: T) = addToKey(map, t, 1)
    def remoteCombine(x: Q, y: Q): Q = {
      val fold = if (x.size < y.size) {
        y.entrySet.iterator.asScala.foldLeft(x) _
      } else {
        x.entrySet.iterator.asScala.foldLeft(y) _
      }
      fold {
        case (map, entry) => addToKey(map, entry.key, entry.value.elem)
      }
    }

    private def addToKey(map: Q, key: T, count: Int): Q = {
      map.get(key) match {
        case null => map.put(key, IntRef.create(count))
        case ref => ref.elem += count
      }
      map
    }
    def remoteFinalize(map: Q) = map.asScala.mapValues(_.elem)

    def localCombine(x: W, y: W): W = {
      val fold = if (x.size < y.size) {
        y.iterator.foldLeft(x) _
      } else {
        x.iterator.foldLeft(y) _
      }
      fold {
        case (map, (key, count)) => map.get(key) match {
          case None => map.updated(key, count)
          case Some(previous) => map.updated(key, previous + count)
        }
      }

    }
    def localFinalize(dist: W): R[T] = dist
  }

}
