package com.hazelcast.Scala

import com.hazelcast.config.MaxSizeConfig

import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy._
import com.hazelcast.memory.MemorySize

sealed trait MaxSize {
  def toConfig: MaxSizeConfig
}

final case class PerNode(maxEntries: Int) extends MaxSize {
  def toConfig = new MaxSizeConfig(maxEntries, PER_NODE)
}
final case class PerPartition(maxEntries: Int) extends MaxSize {
  def toConfig = new MaxSizeConfig(maxEntries, PER_PARTITION)
}
final case class UsedHeapPercentage(percentage: Int) extends MaxSize {
  def toConfig = new MaxSizeConfig(percentage, USED_HEAP_PERCENTAGE)
}
final case class UsedHeapSize(mem: MemorySize) extends MaxSize {
  def toConfig = new MaxSizeConfig(mem.megaBytes.toInt, USED_HEAP_SIZE)
}
final case class FreeHeapPercentage(percentage: Int) extends MaxSize {
  def toConfig = new MaxSizeConfig(percentage, FREE_HEAP_PERCENTAGE)
}
final case class FreeHeapSize(mem: MemorySize) extends MaxSize {
  def toConfig = new MaxSizeConfig(mem.megaBytes.toInt, FREE_HEAP_SIZE)
}
final case class UsedNativeMemoryPercentage(percentage: Int) extends MaxSize {
  def toConfig = new MaxSizeConfig(percentage, USED_NATIVE_MEMORY_PERCENTAGE)
}
final case class UsedNativeMemorySize(mem: MemorySize) extends MaxSize {
  def toConfig = new MaxSizeConfig(mem.megaBytes.toInt, USED_NATIVE_MEMORY_SIZE)
}
final case class FreeNativeMemoryPercentage(percentage: Int) extends MaxSize {
  def toConfig = new MaxSizeConfig(percentage, FREE_NATIVE_MEMORY_PERCENTAGE)
}
final case class FreeNativeMemorySize(mem: MemorySize) extends MaxSize {
  def toConfig = new MaxSizeConfig(mem.megaBytes.toInt, FREE_NATIVE_MEMORY_SIZE)
}
