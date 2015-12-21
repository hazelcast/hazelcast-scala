package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import language.existentials
import com.hazelcast.Scala.dds.Sorted
import scala.collection.mutable.WrappedArray

object Fetch {

  def apply[T: ClassTag]() = new Complete[T]
  private[Scala] def apply[T: ClassTag](sorted: Sorted[T]) = new SortedPartial(sorted)

  final class Complete[T: ClassTag] extends Aggregator[(Array[T], Int), T, (Array[T], Int)] {

    type Q = (Array[T], Int)
    type R = IndexedSeq[T]

    def remoteInit = new Array[T](64) -> 0

    private def increaseCapacity(arr: Array[T], minIncrease: Int): Array[T] = {
      var newLen = arr.length * 2
      while ((newLen - arr.length) < minIncrease) {
        newLen *= 2
      }
      val newArr = new Array[T](newLen)
      System.arraycopy(arr, 0, newArr, 0, arr.length)
      newArr
    }

    def remoteFold(acc: Q, t: T) = {
      val (arr, idx) = acc
      val array = if (idx == arr.length) increaseCapacity(arr, 1) else arr
      array(idx) = t
      array -> (idx + 1)
    }

    private def combine(x: Q, y: Q): Q = {
      val (xArr, xSize) = x
      val xCap = xArr.length - xSize
      val (yArr, ySize) = y
      val yCap = yArr.length - ySize
      val combined = if (xCap >= ySize) {
        System.arraycopy(yArr, 0, xArr, xSize, ySize)
        xArr
      } else if (yCap >= xSize) {
        System.arraycopy(xArr, 0, yArr, ySize, xSize)
        yArr
      } else {
        val newX = increaseCapacity(xArr, ySize)
        System.arraycopy(yArr, 0, newX, xSize, ySize)
        newX
      }
      combined -> (xSize + ySize)
    }
    def remoteCombine(x: Q, y: Q): Q = combine(x, y)

    def remoteFinalize(q: Q) = {
      val (arr, size) = q
      if (arr.length == size) arr -> size
      else {
        val finArr = new Array[T](size)
        System.arraycopy(arr, 0, finArr, 0, size)
        finArr -> size
      }
    }
    def localCombine(x: Q, y: Q) = combine(x, y)
    def localFinalize(q: Q): R = {
      val (arr, size) = q
      val array = if (arr.length == size) arr
      else {
        val array = new Array[T](size)
        System.arraycopy(arr, 0, array, 0, size)
        array
      }
      WrappedArray.make(array)
    }
  }

  final class SortedPartial[T] private[aggr] (sorted: Sorted[T]) extends Aggregator[ArrayBuffer[T], T, List[Array[AnyRef]]] {

    type R = IndexedSeq[T]

    def remoteInit = new ArrayBuffer[T](128)

    def remoteFold(buff: ArrayBuffer[T], t: T) = buff += t

    def remoteCombine(x: ArrayBuffer[T], y: ArrayBuffer[T]) = x ++= y

    private def sortInPlace(array: Array[AnyRef], ordering: Ordering[T]): Array[AnyRef] = {
      java.util.Arrays.sort(array, ordering.asInstanceOf[Ordering[AnyRef]])
      array
    }

    def remoteFinalize(buff: ArrayBuffer[T]) = {
      val array = buff.asInstanceOf[ArrayBuffer[AnyRef]].toArray -> sorted.limit match {
        case (array, Some(limit)) =>
          sortInPlace(array, sorted.ordering)
          val size = sorted.skip + limit
          if (array.length <= size) array
          else array.take(size)
        case (array, _) => array
      }
      List(array)
    }
    def localCombine(x: List[Array[AnyRef]], y: List[Array[AnyRef]]) = x ++ y

    def localFinalize(arrs: List[Array[AnyRef]]): R = {
      val len = arrs.iterator.map(_.length).sum
      if (len == 0) IndexedSeq.empty
      else {
        val result = new Array[AnyRef](len)
        arrs.foldLeft(0) {
          case (offset, part) =>
            System.arraycopy(part, 0, result, offset, part.length)
            offset + part.length
        }
        sortInPlace(result, sorted.ordering)
        val skip = sorted.skip
        val take = sorted.limit.getOrElse(len) min (len - skip)
        if (skip == 0 && take >= result.length) WrappedArray.make[T](result)
        else result.iterator.drop(skip).take(take).map(_.asInstanceOf[T]).toIndexedSeq
      }
    }
  }

  final class Adapter[AQ, T: ClassTag, AW, AR](
    aggr: Aggregator[AQ, T, AW] { type R = AR },
    sorted: Sorted[T])
      extends FinalizeAdapter(new SortedPartial[T](sorted)) {

    type R = AR

    def localFinalize(w: List[Array[AnyRef]]): R = {
      val fetchFinalized = a1.localFinalize(w)
      val folded = fetchFinalized.foldLeft(aggr.remoteInit)(aggr.remoteFold)
      aggr.localFinalize(aggr.remoteFinalize(folded))
    }

  }

}
