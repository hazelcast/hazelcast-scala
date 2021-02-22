package com.hazelcast.Scala

import com.hazelcast.partition.PartitionAware

/**
 * Convenience class for extension of partition-aware keys.
 */
abstract class PartitionKey[T](val getPartitionKey: T) extends PartitionAware[T] with Serializable
