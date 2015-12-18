package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Fetch {
  type Q[T] = ArrayBuffer[T]
  type W[T] = List[Array[T]]
  type R[T] = IndexedSeq[T]

  class Fetch[T: ClassTag](ordering: Option[Ordering[T]], skip: Int, limit: Option[Int]) extends Aggregator[Q[T], T, W[T], R[T]] {

    private[this] val runningMaxSize = limit match {
      case Some(limit) if ordering.isEmpty => limit + skip
      case _ => Int.MaxValue
    }

    def remoteInit = new ArrayBuffer[T](128 min runningMaxSize)

    def remoteFold(buff: ArrayBuffer[T], t: T) = if (buff.size == runningMaxSize) buff else buff += t

    def remoteCombine(x: ArrayBuffer[T], y: ArrayBuffer[T]) =
      if (x.size == runningMaxSize) x
      else if (y.size == runningMaxSize) y
      else x ++= y

    private def sortInPlace(array: Array[T], ordering: Ordering[T]): Array[T] = {
      java.util.Arrays.sort(array.asInstanceOf[Array[Object]], ordering.asInstanceOf[Ordering[Object]])
      array
    }

    def remoteFinalize(buff: ArrayBuffer[T]) = {
      val array: Array[T] = ordering match {
        case None =>
          if (buff.size <= runningMaxSize) buff.toArray
          else buff.iterator.take(runningMaxSize).toArray
        case Some(ordering) =>
          val arr = sortInPlace(buff.toArray, ordering)
          if (arr.length <= runningMaxSize) arr
          else arr.take(runningMaxSize)
      }
      List(array)
    }
    def localCombine(x: List[Array[T]], y: List[Array[T]]) = x ++ y
    def localFinalize(arrs: List[Array[T]]): R[T] = {
      val fullLength = arrs.iterator.map(_.length).sum
      val limit = this.limit.getOrElse(fullLength - skip) min (fullLength - skip)
      if (limit > 0) {
        val result = new Array[T](limit)
        arrs.foldLeft(0, skip, limit) {
          case (t @ (offset, toSkip, toCopy), part) =>
            if (toCopy > 0) {
              if (toSkip >= part.length) {
                (offset + part.length, toSkip - part.length, toCopy)
              } else {
                val copyLen = (part.length - toSkip) min toCopy
                System.arraycopy(part, toSkip, result, offset, copyLen)
                (offset + copyLen, 0, toCopy - copyLen)
              }
            } else t
        }
        result
      } else IndexedSeq.empty
    }
  }
}
