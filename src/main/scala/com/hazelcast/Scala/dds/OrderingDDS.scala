package com.hazelcast.Scala.dds

import scala.concurrent._
import collection.{ Map => cMap }

import com.hazelcast.Scala._

private object OrderingDDS {
  def medianValues[O: Ordering](dist: cMap[O, Freq]): Option[(O, O)] = {
    dist.size match {
      case 0 => None
      case 1 =>
        val value = dist.head._1
        Some(value -> value)
      case _ =>
        val (sorted, virtualLen) = {
          val array = new Array[(O, Int)](dist.size)
          var idx, count = 0
          dist.foreach { tuple =>
            array(idx) = tuple
            count += tuple._2
            idx += 1
          }
          val o = implicitly[Ordering[O]]
          java.util.Arrays.sort(array, new java.util.Comparator[(O, Int)] { def compare(a: (O, Int), b: (O, Int)) = o.compare(a._1, b._1) })
          array -> count
        }
        val even = virtualLen % 2 == 0
        val virtualMedianIdx = (virtualLen + (if (even) 1 else 0)) / 2
        var inclPrevArrayIdx = false
        var virtualIdx, arrayIdx = -1
        while (virtualIdx < virtualMedianIdx) {
          arrayIdx += 1
          virtualIdx += 1
          val subLimit = sorted(arrayIdx)._2 - 1
          var subIdx = 0
          while (subIdx < subLimit && virtualIdx < virtualMedianIdx) {
            subIdx += 1
            virtualIdx += 1
          }
          inclPrevArrayIdx = even && subIdx == 0 && arrayIdx != 0
        }
        assert(virtualIdx == virtualMedianIdx)
        val prevArrOffset = if (inclPrevArrayIdx) 1 else 0
        Some(sorted(arrayIdx - prevArrOffset)._1 -> sorted(arrayIdx)._1)
    }

  }
}

trait OrderingDDS[O] extends AggrDDS[O] {
  implicit protected def ord: Ordering[O]
  def max()(implicit ec: ExecutionContext): Future[Option[O]] = this.maxBy(identity)
  def min()(implicit ec: ExecutionContext): Future[Option[O]] = this.minBy(identity)
  def minMax()(implicit ec: ExecutionContext): Future[Option[(O, O)]] = this.minMaxBy(identity)
  def medianValues()(implicit ec: ExecutionContext): Future[Option[(O, O)]] = distribution().map(OrderingDDS.medianValues[O])
}
trait OrderingGroupDDS[G, O] extends AggrGroupDDS[G, O] {
  implicit protected def ord: Ordering[O]

  def max()(implicit ec: ExecutionContext): Future[cMap[G, O]] = this.maxBy(identity)
  def min()(implicit ec: ExecutionContext): Future[cMap[G, O]] = this.minBy(identity)
  def minMax()(implicit ec: ExecutionContext): Future[cMap[G, (O, O)]] = this.minMaxBy(identity)
  def medianValues()(implicit ec: ExecutionContext): Future[cMap[G, (O, O)]] = distribution().map(_.mapValues(OrderingDDS.medianValues[O](_).get))
}
