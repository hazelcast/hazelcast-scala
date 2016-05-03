package com.hazelcast.Scala

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import com.hazelcast.core._
import com.hazelcast.map.MapPartitionLostEvent
import com.hazelcast.map.listener.MapPartitionLostListener
import com.hazelcast.partition.PartitionLostEvent
import com.hazelcast.partition.PartitionLostListener

private[Scala] trait EventSubscription {
  type ESR
  type MER

  def onLifecycleStateChange(runOn: ExecutionContext = null)(listener: PartialFunction[LifecycleEvent.LifecycleState, Unit]): ESR
  def onDistributedObjectEvent(runOn: ExecutionContext = null)(listener: PartialFunction[DistributedObjectChange, Unit]): ESR
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ESR
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[MigrationEvent, Unit]): ESR
  def onMemberChange(runOn: ExecutionContext = null)(listener: PartialFunction[MemberEvent, Unit]): MER
}

private[Scala] trait MemberEventSubscription extends EventSubscription {

  def onClient(runOn: ExecutionContext = null)(listener: PartialFunction[ClientEvent, Unit]): ESR

}

private[Scala] trait MapEventSubscription {
  type MSR

  def onMapEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[MapEvent, Unit]): MSR
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartialFunction[PartitionLost, Unit]): MSR
}

private[Scala] trait MapEntryEventSubscription[K, V] {
  type MSR

  def onKeyEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[KeyEvent[K], Unit]): MSR
  def onEntryEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[EntryEvent[K, V], Unit]): MSR

  def onKeyEvents(cb: OnKeyEvent[K], localOnly: Boolean): MSR
  final def onKeyEvents(cb: OnKeyEvent[K]): MSR = onKeyEvents(cb, false)
  def onEntryEvents(cb: OnEntryEvent[K, V], localOnly: Boolean): MSR
  final def onEntryEvents(cb: OnEntryEvent[K, V]): MSR = onEntryEvents(cb, false)

}

private[Scala] object EventSubscription {
  def asLifecycleListener(listener: PartialFunction[LifecycleEvent.LifecycleState, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with LifecycleListener {
      def stateChanged(evt: LifecycleEvent): Unit = invokeWith(evt.getState)
    }

  def asDistributedObjectListener(listener: PartialFunction[DistributedObjectChange, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with DistributedObjectListener {
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

  def asPartitionLostListener(listener: PartitionLostEvent => Unit, ec: Option[ExecutionContext]) =
    new PfProxy(PartialFunction(listener), ec) with PartitionLostListener {
      def partitionLost(evt: PartitionLostEvent): Unit = invokeWith(evt)
    }

  def asMigrationListener(listener: PartialFunction[MigrationEvent, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with MigrationListener {
      def migrationCompleted(evt: MigrationEvent) = invokeWith(evt)
      def migrationFailed(evt: MigrationEvent) = invokeWith(evt)
      def migrationStarted(evt: MigrationEvent) = invokeWith(evt)
    }

  def asMembershipListener(
    listener: PartialFunction[MemberEvent, Unit],
    ec: Option[ExecutionContext]): (Future[InitialMembershipEvent], InitialMembershipListener) = {
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

  def asPartitionLostListener(listener: PartialFunction[PartitionLost, Unit], ec: Option[ExecutionContext]): MapPartitionLostListener =
    new PfProxy(listener, ec) with MapPartitionLostListener {
      def partitionLost(evt: MapPartitionLostEvent) = invokeWith(new PartitionLost(evt.getMember, evt.getPartitionId)(evt))
    }

  def asClientListener(listener: PartialFunction[ClientEvent, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with ClientListener {
      def clientConnected(client: Client) = invokeWith(new ClientConnected(client))
      def clientDisconnected(client: Client) = invokeWith(new ClientDisconnected(client))
    }
}
