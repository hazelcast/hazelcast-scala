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
import scala.concurrent.ExecutionContext
import com.hazelcast.map.listener.MapPartitionLostListener
import com.hazelcast.map.MapPartitionLostEvent

private[Scala] trait EventSubscription {
  type ESR
  type MER

  def onLifecycleStateChange(runOn: ExecutionContext = null)(listener: PartialFunction[LifecycleState, Unit]): ESR
  def onDistributedObjectEvent(runOn: ExecutionContext = null)(listener: PartialFunction[DistributedObjectChange, Unit]): ESR
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ESR
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[MigrationEvent, Unit]): ESR
  def onMemberChange(runOn: ExecutionContext = null)(listener: PartialFunction[MemberEvent, Unit]): MER

  protected def asLifecycleListener(listener: PartialFunction[LifecycleState, Unit], ec: Option[ExecutionContext]) = new PfProxy(listener, ec) with LifecycleListener {
    def stateChanged(evt: LifecycleEvent): Unit = invokeWith(evt.getState)
  }
  protected def asDistributedObjectListener(listener: PartialFunction[DistributedObjectChange, Unit], ec: Option[ExecutionContext]) = new PfProxy(listener, ec) with DistributedObjectListener {
    def distributedObjectCreated(evt: DistributedObjectEvent) = hear(evt)
    def distributedObjectDestroyed(evt: DistributedObjectEvent) = hear(evt)
    @inline def hear(evt: DistributedObjectEvent) = {
      import DistributedObjectEvent.EventType._
      val event: DistributedObjectChange = evt.getEventType match {
        case CREATED => DistributedObjectCreated(evt.getObjectName.toString, evt.getDistributedObject)
        case DESTROYED => DistributedObjectDestroyed(evt.getObjectName.toString, evt.getServiceName)
      }
      invokeWith(event)
    }

  }
  protected def asPartitionLostListener(listener: PartitionLostEvent => Unit, ec: Option[ExecutionContext]) = new PfProxy(listener, ec) with PartitionLostListener {
    def partitionLost(evt: PartitionLostEvent): Unit = invokeWith(evt)
  }

  protected def asMigrationListener(listener: PartialFunction[MigrationEvent, Unit], ec: Option[ExecutionContext]) = new PfProxy(listener, ec) with MigrationListener {
    def migrationCompleted(evt: MigrationEvent) = invokeWith(evt)
    def migrationFailed(evt: MigrationEvent) = invokeWith(evt)
    def migrationStarted(evt: MigrationEvent) = invokeWith(evt)
  }

  protected def asMembershipListener(listener: PartialFunction[MemberEvent, Unit], ec: Option[ExecutionContext]): (Future[InitialMembershipEvent], InitialMembershipListener) = {
    import com.hazelcast.cluster.MemberAttributeOperationType._
    import MembershipEvent._
    import collection.JavaConverters._

    val promise = Promise[InitialMembershipEvent]
    promise.future -> new PfProxy(listener, ec) with InitialMembershipListener {
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
        invokeWith(event)
      }
    }
  }

}

private[Scala] trait MemberEventSubscription extends EventSubscription {

  def onClient(runOn: ExecutionContext = null)(listener: PartialFunction[ClientEvent, Unit]): ESR

  protected def asClientListener(listener: PartialFunction[ClientEvent, Unit], ec: Option[ExecutionContext]) = new PfProxy(listener, ec) with ClientListener {
    def clientConnected(client: Client) = invokeWith(new ClientConnected(client))
    def clientDisconnected(client: Client) = invokeWith(new ClientDisconnected(client))
  }

}

private[Scala] trait MapEventSubscription {
  type MSR

  def onMapEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[MapEvent, Unit]): MSR

  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartialFunction[PartitionLost, Unit]): MSR
  protected def asPartitionLostListener(listener: PartialFunction[PartitionLost, Unit], ec: Option[ExecutionContext]): MapPartitionLostListener =
    new PfProxy(listener, ec) with MapPartitionLostListener {
      def partitionLost(evt: MapPartitionLostEvent) = invokeWith(new PartitionLost(evt.getMember, evt.getPartitionId)(evt))
    }
}

private[Scala] trait MapEntryEventSubscription[K, V] {
  type MSR
  def onKeyEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[KeyEvent[K], Unit]): MSR
  def onEntryEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[EntryEvent[K, V], Unit]): MSR

}
