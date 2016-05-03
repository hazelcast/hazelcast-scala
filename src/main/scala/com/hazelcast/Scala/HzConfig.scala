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
import scala.concurrent.ExecutionContext

class HzConfig(conf: Config) extends MemberEventSubscription {
  type ESR = Config
  def userCtx: UserContext = new UserContext(conf.getUserContext)
  def newInstance(): HazelcastInstance = Hazelcast.newHazelcastInstance(conf)
  def getInstance(): HazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(conf)

  def onClient(runOn: ExecutionContext = null)(listener: PartialFunction[ClientEvent, Unit]): Config =
    conf addListenerConfig new ListenerConfig(EventSubscription.asClientListener(listener, Option(runOn)))
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
