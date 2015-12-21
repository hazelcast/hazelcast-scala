package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import language.existentials
import com.hazelcast.Scala.dds.Sorted

object Fetch {

  type Q[T] = ArrayBuffer[T]
  type W[T] = List[Array[_]]
  type R[T] = IndexedSeq[T]

  def apply[T: ClassTag](sorted: Option[Sorted[T]] = None) = new Fetch(sorted)

  class Fetch[T: ClassTag](sorted: Option[Sorted[T]]) extends Aggregator[Q[T], T, W[T], R[T]] {

    def remoteInit = new ArrayBuffer[T](128)

    def remoteFold(buff: ArrayBuffer[T], t: T) = buff += t

    def remoteCombine(x: ArrayBuffer[T], y: ArrayBuffer[T]) = x ++= y

    private def sortInPlace(array: Array[_], ordering: Ordering[T]): Array[_] = {
      java.util.Arrays.sort(array.asInstanceOf[Array[Object]], ordering.asInstanceOf[Ordering[Object]])
      array
    }

    def remoteFinalize(buff: ArrayBuffer[T]) = {
      val array: Array[_] = sorted match {
        case Some(Sorted(ordering, skip, Some(limit))) =>
          val arr = sortInPlace(buff.iterator.map(_.asInstanceOf[Object]).toArray, ordering)
          val size = skip + limit
          if (arr.length <= size) arr
          else arr.take(size)
        case _ => buff.toArray
      }
      List(array)
    }
    def localCombine(x: List[Array[_]], y: List[Array[_]]) = x ++ y
    def localFinalize(arrs: List[Array[_]]): R[T] = {
      val fullLength = arrs.iterator.map(_.length).sum
      val (skip, limit) = sorted match {
        case None => 0 -> fullLength
        case Some(Sorted(_, skip, limit)) =>
          skip -> (limit.getOrElse(fullLength - skip) min (fullLength - skip))
      }
      if (limit > 0) {
        val result = new Array[T](limit)
        arrs.foldLeft((0, skip, limit)) {
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

  final class Adapter[AQ, T: ClassTag, AW, AR](
    aggr: Aggregator[AQ, T, AW, AR],
    sorted: Sorted[T])
      extends FinalizeAdapter[T, AR, Q[T], W[T], R[T]](new Fetch[T](Some(sorted))) {

    def localFinalize(w: W[T]): AR = {
      val folded: AQ = a1.localFinalize(w).foldLeft(aggr.remoteInit)(aggr.remoteFold)
      aggr.localFinalize(aggr.remoteFinalize(folded))
    }

  }

}
