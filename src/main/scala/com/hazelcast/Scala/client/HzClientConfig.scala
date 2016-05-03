package com.hazelcast.Scala.client

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.config.ListenerConfig
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.InitialMembershipEvent
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.core.MigrationEvent
import com.hazelcast.partition.PartitionLostEvent
import com.hazelcast.Scala.DistributedObjectChange
import com.hazelcast.Scala.EventSubscription
import com.hazelcast.Scala.MemberEvent

class HzClientConfig(conf: ClientConfig) extends EventSubscription {
  type ESR = ClientConfig

  def newClient(): HazelcastInstance = HazelcastClient.newHazelcastClient(conf)

  def onLifecycleStateChange(runOn: ExecutionContext = null)(listener: PartialFunction[LifecycleState, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(EventSubscription.asLifecycleListener(listener, Option(runOn)))
  def onDistributedObjectEvent(runOn: ExecutionContext = null)(listener: PartialFunction[DistributedObjectChange, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(EventSubscription.asDistributedObjectListener(listener, Option(runOn)))
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ESR =
    conf addListenerConfig new ListenerConfig(EventSubscription.asPartitionLostListener(listener, Option(runOn)))
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[MigrationEvent, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(EventSubscription.asMigrationListener(listener, Option(runOn)))

  type MER = Future[InitialMembershipEvent]
  def onMemberChange(runOn: ExecutionContext = null)(listener: PartialFunction[MemberEvent, Unit]): MER = {
    val (future, mbrListener) = EventSubscription.asMembershipListener(listener, Option(runOn))
    conf addListenerConfig new ListenerConfig(mbrListener)
    future
  }

}
