package com.hazelcast.Scala.aggr

import com.hazelcast.Scala.Aggregation
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class Fetch[T: ClassTag] extends Aggregation[ArrayBuffer[T], T, List[Array[T]], Seq[T]] {
  def remoteInit = new ArrayBuffer[T](128)
  def remoteFold(buff: ArrayBuffer[T], t: T) = buff += t
  def remoteCombine(x: ArrayBuffer[T], y: ArrayBuffer[T]) = x ++= y
  def remoteFinalize(buff: ArrayBuffer[T]) = List(buff.toArray)
  def localCombine(x: List[Array[T]], y: List[Array[T]]) = x ++ y
  def localFinalize(arrs: List[Array[T]]): Seq[T] = {
    val length = arrs.map(_.length).sum
    val (result, _) = arrs.foldLeft(new Array[T](length) -> 0) {
      case ((result, offset), part) =>
        System.arraycopy(part, 0, result, offset, part.length)
        result -> (offset + part.length)
    }
    result
  }
}
