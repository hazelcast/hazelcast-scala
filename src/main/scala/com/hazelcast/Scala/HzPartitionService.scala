package com.hazelcast.Scala

import scala.concurrent.ExecutionContext
import com.hazelcast.partition.{PartitionLostEvent, PartitionService, ReplicaMigrationEvent}

class HzPartitionService(private val service: PartitionService) extends AnyVal {
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ListenerRegistration = {
    val regId = service addPartitionLostListener EventSubscription.asPartitionLostListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel(): Boolean = service removePartitionLostListener regId
    }
  }
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[ReplicaMigrationEvent, Unit]): ListenerRegistration = {
    val regId = service addMigrationListener EventSubscription.asMigrationListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel(): Boolean = service removeMigrationListener regId
    }
  }

}
