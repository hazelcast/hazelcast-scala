package com.hazelcast.Scala

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.Hazelcast
import com.hazelcast.config.Config
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.core.DistributedObjectEvent
import com.hazelcast.partition.PartitionLostEvent
import com.hazelcast.core.MigrationEvent
import com.hazelcast.config.ListenerConfig
import scala.concurrent.Future
import com.hazelcast.core.InitialMembershipEvent
import com.hazelcast.core.MembershipEvent

class HzConfig(conf: Config) extends MemberEventSubscription {
  type ESR = Config
  def userCtx: UserContext = new UserContext(conf.getUserContext)
  def newInstance(): HazelcastInstance = Hazelcast.newHazelcastInstance(conf)
  def getInstance(): HazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(conf)

  def onClient(listener: PartialFunction[ClientEvent, Unit]): Config =
    conf addListenerConfig new ListenerConfig(asClientListener(listener))
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
