package com.hazelcast.Scala

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.client.HazelcastClient
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.core.DistributedObjectEvent
import com.hazelcast.config.ListenerConfig
import com.hazelcast.partition.PartitionLostEvent
import com.hazelcast.core.MigrationEvent
import scala.concurrent.Future
import com.hazelcast.core.MembershipEvent
import com.hazelcast.core.InitialMembershipEvent

class HzClientConfig(conf: ClientConfig) extends EventSubscription {
  type ESR = ClientConfig

  def newClient(): HazelcastInstance = HazelcastClient.newHazelcastClient(conf)

  def onLifecycleStateChange(listener: PartialFunction[LifecycleState, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(asLifecycleListener(listener))
  def onDistributedObjectEvent(listener: PartialFunction[DistributedObjectChange, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(asDistributedObjectListener(listener))
  def onPartitionLost(listener: PartitionLostEvent => Unit): ESR =
    conf addListenerConfig new ListenerConfig(asPartitionLostListener(listener))
  def onMigration(listener: PartialFunction[MigrationEvent, Unit]): ESR =
    conf addListenerConfig new ListenerConfig(asMigrationListener(listener))

  type MER = Future[InitialMembershipEvent]
  def onMemberChange(listener: PartialFunction[MemberEvent, Unit]): MER = {
    val (future, mbrListener) = asMembershipListener(listener)
    conf addListenerConfig new ListenerConfig(mbrListener)
    future
  }

}
