package com.hazelcast.Scala

import com.hazelcast.config.MapConfig
import scala.concurrent.ExecutionContext
import com.hazelcast.config.EntryListenerConfig

class HzTypedMapConfig[K, V](mapConfig: MapConfig) extends MapEntryEventSubscription[K, V] with MapEventSubscription {
  type MSR = this.type
  def onKeyEvents(localOnly: Boolean, runOn: ExecutionContext)(pf: PartialFunction[KeyEvent[K], Unit]): MSR = {
    val keyListener = new KeyListener(pf, Option(runOn))
    mapConfig addEntryListenerConfig new EntryListenerConfig(keyListener, localOnly, false)
    this
  }
  def onEntryEvents(localOnly: Boolean, runOn: ExecutionContext)(pf: PartialFunction[EntryEvent[K, V], Unit]): MSR = {
    val entryListener = new EntryListener(pf, Option(runOn))
    mapConfig addEntryListenerConfig new EntryListenerConfig(entryListener, localOnly, true)
    this
  }
  def onMapEvents(localOnly: Boolean, runOn: ExecutionContext)(pf: PartialFunction[MapEvent, Unit]): MSR = {
    mapConfig.onMapEvents(localOnly, runOn)(pf)
    this
  }
  def onPartitionLost(runOn: ExecutionContext)(listener: PartialFunction[PartitionLost, Unit]): MSR = {
    mapConfig.onPartitionLost(runOn)(listener)
    this
  }

}
