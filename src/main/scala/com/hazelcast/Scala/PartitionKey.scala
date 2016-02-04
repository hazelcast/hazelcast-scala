package com.hazelcast.Scala

import scala.beans.BeanProperty
import com.hazelcast.core.PartitionAware
import scala.beans.BeanInfo

/**
 * Convenience class for extension of partition-aware keys.
 */
abstract class PartitionKey[T](val getPartitionKey: T) extends PartitionAware[T]
