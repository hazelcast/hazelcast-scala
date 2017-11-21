package com.hazelcast.Scala

import com.hazelcast.ringbuffer._
import collection.JavaConverters._
import scala.concurrent._
import com.hazelcast.core.IFunction
import com.hazelcast.core.ExecutionCallback
import com.hazelcast.ringbuffer.impl.RingbufferProxy
import java.util.concurrent.Executor

private object AsyncRingbuffer {
  implicit val jl2osl = (jl: java.lang.Long) => if (jl == -1L) None else Some(jl: Long)
  def MaxBatchSize = RingbufferProxy.MAX_BATCH_SIZE
}

class AsyncRingbuffer[E](private val rb: Ringbuffer[E]) extends AnyVal {
  import AsyncRingbuffer._

  /**
    * Add item.
    * @param item The item to add
    * @param overflowPolicy The overflow policy, defaults to `OVERWRITE`
    * @return The sequence number added. Will only return `None` if default policy is `FAIL` and capacity is reached.
    */
  def add(item: E, overflowPolicy: OverflowPolicy = OverflowPolicy.OVERWRITE): Future[Option[Long]] = {
    rb.addAsync(item, overflowPolicy).asScala
  }

  /**
    * Add batch of items.
    * @param items The items to add
    * @param overflowPolicy The overflow policy, defaults to `OVERWRITE`
    * @return The last sequence number added. Will only return `None` if default policy is `FAIL` and capacity is reached.
    */
  def addAll(items: Iterable[E], overflowPolicy: OverflowPolicy = OverflowPolicy.OVERWRITE): Future[Option[Long]] = {
    rb.addAllAsync(items.asJavaCollection, overflowPolicy).asScala
  }

  /**
    * Read batch of items. If minimum item count is > 0, then this method will
    * block until at least the minimum count is available.
    * @param startFrom The sequence number to read from
    * @param minItems The min number of items to read.
    * @param pf The callback function
    * @return The read count, i.e. the number of sequences processed, regardless of callback function.
    */
  def readBatch(
    startFrom: Long,
    minItems: Int)(pf: PartialFunction[E, Unit]): Future[Int] = readBatch(startFrom, minItems to MaxBatchSize)(pf)

  /**
    * Read batch of items. If minimum item count is > 0, then this method will
    * block until at least the minimum count is available.
    * @param startFrom The sequence number to read from
    * @param numberOfItems The min and max number of items to read. Defaults to at least 1
    * @param pf The callback function
    * @return The read count, i.e. the number of sequences processed, regardless of callback function.
    */
  def readBatch(
    startFrom: Long,
    numberOfItems: Range = 1 to MaxBatchSize)(pf: PartialFunction[E, Unit]): Future[Int] = {
    val filter = new IFunction[E, java.lang.Boolean] {
      def apply(item: E) = pf.isDefinedAt(item)
    }
    val promise = Promise[Int]
    rb.readManyAsync(startFrom, numberOfItems.head, numberOfItems.last, filter) andThen new ExecutionCallback[ReadResultSet[E]] {
      def onResponse(result: ReadResultSet[E]) = {
        try {
          result.asScala.foreach(pf)
          promise success result.readCount
        } catch {
          case t: Throwable => onFailure(t)
        }
      }
      def onFailure(t: Throwable) = promise failure t
    }
    promise.future
  }
}
