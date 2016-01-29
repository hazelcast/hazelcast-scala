package com.hazelcast.Scala.xtra

import com.hazelcast.quorum.Quorum
import com.hazelcast.core.Cluster
import java.util.concurrent.atomic.AtomicInteger
import com.hazelcast.core.IAtomicLong
import com.hazelcast.quorum.QuorumFunction
import com.hazelcast.core.Member
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.logging.ILogger
import collection.JavaConversions._
import java.util.Collections
import java.util.WeakHashMap

/**
  * Quorum function for elastic clusters.
  * @param targetClusterSize The distributed atomic long holding the target cluster size
  * @param safe Ensure that target cluster resizing maintains quorum under normal operations. Defaults to `true`.
  */
class ElasticClusterQuorum(safe: Boolean = true) extends QuorumFunction {
  @volatile private var hz: HazelcastInstance = _
  @volatile private var logger: ILogger = _
  @volatile private var targetClusterSize: IAtomicLong = _
  private def targetSize = targetClusterSize.get.toInt
  private def minRequiredSize = (targetSize / 2) + 1
  def apply(members: java.util.Collection[Member]): Boolean = {
    val observedSize = members.size
    if (observedSize > targetSize) {

      val newTargetSize = changeTargetClusterSize(observedSize)
      if (observedSize > newTargetSize) {
        // FIXME: Any alternative?
        sys.error(s"Observed cluster size of $observedSize is bigger than target cluster size $newTargetSize and cannot be safely incremented")
      }
    }
    observedSize >= minRequiredSize
  }

  def init(hz: HazelcastInstance, targetClusterSize: Int): Unit = {
    val name = s"hz:${classOf[ElasticClusterQuorum].getName}"
    this.targetClusterSize = hz.getAtomicLong(name)
    this.logger = hz.getLoggingService.getLogger(name)
    this.hz = hz
    changeTargetClusterSize(targetClusterSize)
  }

  /**
    * Change target cluster size, i.e.
    * the expected size of cluster.
    * @param newSize The new desired target cluster size
    * @return The new actual target cluster size. NOTE: This may be different than what's passed
    */
  def changeTargetClusterSize(newSize: Int): Int = {
    val currSize = targetSize
    if (newSize <= 0 || currSize == newSize) currSize
    else {
      val expectedChange = newSize - currSize
      val newActualSize =
        if (expectedChange > 0) { // Increase cluster
          val expectedIncrease = expectedChange
          val actualIncrease = if (safe) {
            val maxIncrease = currSize - 1
            maxIncrease min expectedIncrease
          } else expectedIncrease
          currSize + actualIncrease
        } else { // Decrease cluster
          val expectedDecrease = -expectedChange
          val actualDecrease = if (safe) {
            val maxDecrease = currSize / 2
            maxDecrease min expectedDecrease
          } else expectedDecrease
          currSize - actualDecrease
        }
      targetClusterSize.compareAndSet(currSize, newActualSize)
      targetSize
    }
  }
}
