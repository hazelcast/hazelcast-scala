package com.hazelcast.Scala

import com.hazelcast.core.Client
import com.hazelcast.core.ClientListener
import com.hazelcast.core.DistributedObjectEvent
import com.hazelcast.core.DistributedObjectListener
import com.hazelcast.core.LifecycleEvent
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.core.LifecycleListener
import com.hazelcast.core.MembershipEvent
import com.hazelcast.core.MigrationEvent
import com.hazelcast.core.MigrationListener
import com.hazelcast.partition.PartitionLostEvent
import com.hazelcast.partition.PartitionLostListener
import com.hazelcast.core.MembershipListener
import com.hazelcast.core.InitialMembershipListener
import com.hazelcast.core.MemberAttributeEvent
import com.hazelcast.core.InitialMembershipEvent
import scala.concurrent.Future
import scala.concurrent.Promise

private[Scala] trait EventSubscription {
  type ESR
  type MER

  def onLifecycleStateChange(listener: PartialFunction[LifecycleState, Unit]): ESR
  def onDistributedObjectEvent(listener: PartialFunction[DistributedObjectChange, Unit]): ESR
  def onPartitionLost(listener: PartitionLostEvent => Unit): ESR
  def onMigration(listener: PartialFunction[MigrationEvent, Unit]): ESR
  def onMemberChange(listener: PartialFunction[MemberEvent, Unit]): MER

  protected def asLifecycleListener(listener: PartialFunction[LifecycleState, Unit]) = new LifecycleListener {
    def stateChanged(evt: LifecycleEvent): Unit =
      if (listener isDefinedAt evt.getState) listener(evt.getState)
  }
  protected def asDistributedObjectListener(listener: PartialFunction[DistributedObjectChange, Unit]) = new DistributedObjectListener {
    def distributedObjectCreated(evt: DistributedObjectEvent) = hear(evt)
    def distributedObjectDestroyed(evt: DistributedObjectEvent) = hear(evt)
    @inline def hear(evt: DistributedObjectEvent) = {
      import DistributedObjectEvent.EventType._
      val event: DistributedObjectChange = evt.getEventType match {
        case CREATED => DistributedObjectCreated(evt.getObjectName.toString, evt.getDistributedObject)
        case DESTROYED => DistributedObjectDestroyed(evt.getObjectName.toString, evt.getServiceName)
      }
      if (listener isDefinedAt event) listener(event)
    }

  }
  protected def asPartitionLostListener(listener: PartitionLostEvent => Unit) = new PartitionLostListener {
    def partitionLost(evt: PartitionLostEvent): Unit = listener(evt)
  }

  protected def asMigrationListener(listener: PartialFunction[MigrationEvent, Unit]) = new MigrationListener {
    def migrationCompleted(evt: MigrationEvent) = hear(evt)
    def migrationFailed(evt: MigrationEvent) = hear(evt)
    def migrationStarted(evt: MigrationEvent) = hear(evt)
    @inline def hear(evt: MigrationEvent): Unit =
      if (listener isDefinedAt evt) listener(evt)
  }

  protected def asMembershipListener(listener: PartialFunction[MemberEvent, Unit]): (Future[InitialMembershipEvent], InitialMembershipListener) = {
    import com.hazelcast.cluster.MemberAttributeOperationType._
    import MembershipEvent._
    import collection.JavaConverters._

    val promise = Promise[InitialMembershipEvent]
    promise.future -> new InitialMembershipListener {
      def init(evt: InitialMembershipEvent) = promise success evt
      def memberAdded(evt: MembershipEvent) = hear(evt)
      def memberRemoved(evt: MembershipEvent) = hear(evt)
      def memberAttributeChanged(evt: MemberAttributeEvent) = hear(evt)
      private def hear(evt: MembershipEvent) = {
        val event: MemberEvent = evt match {
          case evt: MemberAttributeEvent if evt.getOperationType == PUT && evt.getValue != null => MemberAttributeUpdated(evt.getMember, evt.getKey, evt.getValue)(evt.getCluster)
          case evt: MemberAttributeEvent if evt.getOperationType == REMOVE || evt.getValue == null => MemberAttributeRemoved(evt.getMember, evt.getKey)(evt.getCluster)
          case evt: MembershipEvent if evt.getEventType == MEMBER_ADDED => MemberAdded(evt.getMember, evt.getMembers.asScala)(evt.getCluster)
          case evt: MembershipEvent if evt.getEventType == MEMBER_REMOVED => MemberRemoved(evt.getMember, evt.getMembers.asScala)(evt.getCluster)
        }
        if (listener.isDefinedAt(event)) listener(event)
      }
    }
  }

}

private[Scala] trait MemberEventSubscription extends EventSubscription {

  def onClient(listener: PartialFunction[ClientEvent, Unit]): ESR

  protected def asClientListener(listener: PartialFunction[ClientEvent, Unit]) = new ClientListener {
    def clientConnected(client: Client) {
      val evt = new ClientConnected(client)
      if (listener isDefinedAt evt) listener(evt)
    }
    def clientDisconnected(client: Client) {
      val evt = new ClientDisconnected(client)
      if (listener isDefinedAt evt) listener(evt)
    }
  }

}
