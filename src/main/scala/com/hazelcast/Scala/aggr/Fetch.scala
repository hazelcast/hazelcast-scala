package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregator
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import language.existentials
import com.hazelcast.Scala.dds.Sorted
import scala.collection.mutable.WrappedArray

object Fetch {

  def apply[T: ClassTag]() = new Complete[T]

  private[Scala] def apply[T](sorted: Sorted[T]) =
    sorted.limit match {
      case None => new SortedUnlimited(sorted.ordering, sorted.skip)
      case Some(limit) => new SortedPartial(sorted.ordering, sorted.skip, limit)
    }

  final class Complete[T: ClassTag] extends Aggregator[T, IndexedSeq[T]] {

    type Q = (Array[T], Int)
    type W = (Array[T], Int)
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

  private[Scala] sealed abstract class SortedFetcher[T, AQ](init: => AQ) extends Aggregator[T, IndexedSeq[T]] {
    final type Q = AQ
    final type W = List[Array[AnyRef]]
    final type R = IndexedSeq[T]
    final def remoteInit = init
  }

  private[Scala] final class SortedPartial[T](ordering: Ordering[T], skip: Int, limit: Int)
      extends SortedFetcher[T, (Array[AnyRef], Int)](new Array[AnyRef](skip + limit) -> 0) {

    private[this] val anyRefOrd = ordering.asInstanceOf[Ordering[AnyRef]]
    private def binarySearch(arr: Array[AnyRef], obj: T): Int = {
      java.util.Arrays.binarySearch(arr, obj.asInstanceOf[AnyRef], anyRefOrd)
    }
    private def insertElement(arr: Array[AnyRef], obj: T, pos: Int) {
      if (pos != arr.length - 1) {
        System.arraycopy(arr, pos, arr, pos + 1, arr.length - pos - 1)
      }
      arr(pos) = obj.asInstanceOf[AnyRef]
    }

    def remoteFold(q: Q, t: T) = {
      val (arr, len) = q
      if (len == arr.length) {
        val pos = binarySearch(arr, t)
        if (~pos == len) {} // Ignore, out of range
        else if (pos == len - 1) {} // Ignore, last place already occupied by duplicate
        else if (pos < 0) insertElement(arr, t, ~pos)
        else insertElement(arr, t, pos)
        q
      } else {
        arr(len) = t.asInstanceOf[AnyRef]
        if (len == arr.length - 1) {
          java.util.Arrays.sort(arr, anyRefOrd)
        }
        arr -> (len + 1)
      }
    }

    private def moveAtoB(a: Q, b: Q): Q = {
      val toMove = if (a._1.length == a._2) {
        a._1.iterator.takeWhile { a =>
          b._1(b._1.length - 1) match {
            case null => true
            case b => anyRefOrd.lt(a, b)
          }
        }
      } else {
        a._1.iterator.takeWhile(_ != null)
      }
      toMove.map(_.asInstanceOf[T]).foldLeft(b)(remoteFold)
    }

    def remoteCombine(x: Q, y: Q) = {
      val (xArr, xLen) = x
      val (yArr, yLen) = y
      if (xArr.length == xLen) { // x is ordered and complete
        if (yArr.length == yLen) { // both are ordered and complete
          if (anyRefOrd.lteq(xArr(xLen - 1), yArr(0))) x
          else if (anyRefOrd.lteq(yArr(yLen - 1), xArr(0))) y
          else if (anyRefOrd.lt(xArr(0), yArr(0))) {
            moveAtoB(y, x)
          } else {
            moveAtoB(x, y)
          }
        } else { // only x is sorted and complete
          moveAtoB(y, x)
        }
      } else if (yArr.length == yLen) { // only y is ordered and complete
        moveAtoB(x, y)
      } else if (yLen < xLen) { // neither is ordered nor complete, y is smaller
        moveAtoB(y, x)
      } else {
        moveAtoB(x, y)
      }
    }

    def remoteFinalize(q: Q) = {
      val (arr, len) = q
      if (arr.length == len) List(arr)
      else {
        val less = arr.take(len)
        java.util.Arrays.sort(less, anyRefOrd)
        List(less)
      }
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
        java.util.Arrays.sort(result, anyRefOrd)
        val take = limit min (len - skip)
        if (skip == 0 && take >= result.length) WrappedArray.make[T](result)
        else result.iterator.drop(skip).take(take).map(_.asInstanceOf[T]).toIndexedSeq
      }
    }
  }

  private[Scala] final class SortedUnlimited[T](ordering: Ordering[T], skip: Int)
      extends SortedFetcher[T, ArrayBuffer[T]](new ArrayBuffer[T](128)) {

    def remoteFold(buff: ArrayBuffer[T], t: T) = buff += t

    def remoteCombine(x: ArrayBuffer[T], y: ArrayBuffer[T]) = x ++= y

    def remoteFinalize(buff: ArrayBuffer[T]) = {
      val array = buff.asInstanceOf[ArrayBuffer[AnyRef]].toArray
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
        java.util.Arrays.sort(result, ordering.asInstanceOf[Ordering[AnyRef]])
        if (skip == 0) WrappedArray.make[T](result)
        else result.iterator.drop(skip).map(_.asInstanceOf[T]).toIndexedSeq
      }
    }
  }

  def Adapter[T, R](
    aggr: Aggregator[T, R],
    sorted: Sorted[T]) = {
    new Adapter(aggr, apply(sorted))
  }

  final class Adapter[T, R, Q] private[Fetch] (
    aggr: Aggregator[T, R],
    fetcher: SortedFetcher[T, Q])
      extends FinalizeAdapter[T, R, IndexedSeq[T]](fetcher) {

    def localFinalize(w: W): R = {
      val fetchFinalized = a1.localFinalize(w)
      val folded = fetchFinalized.foldLeft(aggr.remoteInit)(aggr.remoteFold)
      aggr.localFinalize(aggr.remoteFinalize(folded))
    }

  }

}
