package com.hazelcast.Scala

import java.util.EventListener

import scala.concurrent.{ ExecutionContext, Future }

import com.hazelcast.config.{ Config, ListenerConfig }
import com.hazelcast.core._
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.core.MigrationEvent
import com.hazelcast.partition.PartitionLostEvent

class HzConfig(conf: Config) extends MemberEventSubscription {
  type ESR = Config
  def userCtx: UserContext = new UserContext(conf.getUserContext)
  def newInstance(): HazelcastInstance = Hazelcast.newHazelcastInstance(conf)
  def getInstance(): HazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(conf)

  def addListener(l: EventListener): ESR =
    conf addListenerConfig new ListenerConfig(l)
  def onClient(runOn: ExecutionContext = null)(listener: PartialFunction[ClientEvent, Unit]): Config =
    conf addListener EventSubscription.asClientListener(listener, Option(runOn))
  def onLifecycleStateChange(runOn: ExecutionContext = null)(listener: PartialFunction[LifecycleState, Unit]): ESR =
    conf addListener EventSubscription.asLifecycleListener(listener, Option(runOn))
  def onDistributedObjectEvent(runOn: ExecutionContext = null)(listener: PartialFunction[DistributedObjectChange, Unit]): ESR =
    conf addListener EventSubscription.asDistributedObjectListener(listener, Option(runOn))
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ESR =
    conf addListener EventSubscription.asPartitionLostListener(listener, Option(runOn))
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[MigrationEvent, Unit]): ESR =
    conf addListener EventSubscription.asMigrationListener(listener, Option(runOn))

  type MER = Future[InitialMembershipEvent]
  def onMemberChange(runOn: ExecutionContext = null)(listener: PartialFunction[MemberEvent, Unit]): MER = {
    val (future, mbrListener) = EventSubscription.asMembershipListener(listener, Option(runOn))
    conf addListener mbrListener
    future
  }
}
