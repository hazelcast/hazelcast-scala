package com.hazelcast.Scala

import com.hazelcast.core.PartitionService
import scala.concurrent.ExecutionContext
import com.hazelcast.partition.PartitionLostEvent
import com.hazelcast.core.MigrationEvent

class HzPartitionService(private val service: PartitionService) extends AnyVal {
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ListenerRegistration = {
    val regId = service addPartitionLostListener EventSubscription.asPartitionLostListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel(): Boolean = service removePartitionLostListener regId
    }
  }
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[MigrationEvent, Unit]): ListenerRegistration = {
    val regId = service addMigrationListener EventSubscription.asMigrationListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel(): Boolean = service removeMigrationListener regId
    }
  }

}
