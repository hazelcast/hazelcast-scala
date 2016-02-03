package com.hazelcast.Scala

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

class HzClientConfig(conf: ClientConfig) extends EventSubscription {
  type ESR = ClientConfig

  def newClient(): HazelcastInstance = HazelcastClient.newHazelcastClient(conf)

  def onLifecycleStateChange(runOn: ExecutionContext)(listener: PartialFunction[LifecycleState, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(asLifecycleListener(listener, Option(runOn)))
  def onDistributedObjectEvent(runOn: ExecutionContext)(listener: PartialFunction[DistributedObjectChange, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(asDistributedObjectListener(listener, Option(runOn)))
  def onPartitionLost(runOn: ExecutionContext)(listener: PartitionLostEvent => Unit): ESR =
    conf addListenerConfig new ListenerConfig(asPartitionLostListener(listener, Option(runOn)))
  def onMigration(runOn: ExecutionContext)(listener: PartialFunction[MigrationEvent, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(asMigrationListener(listener, Option(runOn)))

  type MER = Future[InitialMembershipEvent]
  def onMemberChange(runOn: ExecutionContext)(listener: PartialFunction[MemberEvent, Unit]): MER = {
    val (future, mbrListener) = asMembershipListener(listener, Option(runOn))
    conf addListenerConfig new ListenerConfig(mbrListener)
    future
  }

}
