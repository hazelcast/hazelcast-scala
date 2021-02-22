package com.hazelcast.Scala

import com.hazelcast.cluster.{Cluster, InitialMembershipEvent}
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class HzCluster(private val cluster: Cluster) extends AnyVal {

  def onMemberChange(runOn: ExecutionContext = null)(listener: PartialFunction[MemberEvent, Unit]): (ListenerRegistration, Future[InitialMembershipEvent]) = {
    val (future, mbrListener) = EventSubscription.asMembershipListener(listener, Option(runOn))
    val regId = cluster addMembershipListener mbrListener
    new ListenerRegistration {
      def cancel(): Boolean = cluster removeMembershipListener regId
    } -> future
  }

}
