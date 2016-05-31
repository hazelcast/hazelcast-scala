package com.hazelcast.Scala

import com.hazelcast.config.MaxSizeConfig

import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy._
import com.hazelcast.memory.MemorySize

object PerNode {
  def apply(maxEntries: Int) = new MaxSizeConfig(maxEntries, PER_NODE)
}
object PerPartition {
  def apply(maxEntries: Int) = new MaxSizeConfig(maxEntries, PER_PARTITION)
}
object UsedHeapPercentage {
  def apply(percentage: Int) = new MaxSizeConfig(percentage, USED_HEAP_PERCENTAGE)
}
object UsedHeapSize {
  def apply(mem: MemorySize) = new MaxSizeConfig(mem.megaBytes.toInt, USED_HEAP_SIZE)
}
object FreeHeapPercentage {
  def apply(percentage: Int) = new MaxSizeConfig(percentage, FREE_HEAP_PERCENTAGE)
}
object FreeHeapSize {
  def apply(mem: MemorySize) = new MaxSizeConfig(mem.megaBytes.toInt, FREE_HEAP_SIZE)
}
object UsedNativeMemoryPercentage {
  def apply(percentage: Int) = new MaxSizeConfig(percentage, USED_NATIVE_MEMORY_PERCENTAGE)
}
object UsedNativeMemorySize {
  def apply(mem: MemorySize) = new MaxSizeConfig(mem.megaBytes.toInt, USED_NATIVE_MEMORY_SIZE)
}
object FreeNativeMemoryPercentage {
  def apply(percentage: Int) = new MaxSizeConfig(percentage, FREE_NATIVE_MEMORY_PERCENTAGE)
}
object FreeNativeMemorySize {
  def apply(mem: MemorySize) = new MaxSizeConfig(mem.megaBytes.toInt, FREE_NATIVE_MEMORY_SIZE)
}
