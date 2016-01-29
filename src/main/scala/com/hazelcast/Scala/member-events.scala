package com.hazelcast.Scala

import com.hazelcast.core.Member
import scala.collection.{ Set => aSet }
import com.hazelcast.core.Cluster

sealed abstract class MemberEvent(val cluster: Cluster)

case class MemberAdded(added: Member, result: aSet[Member])(cluster: Cluster) extends MemberEvent(cluster)
case class MemberRemoved(removed: Member, result: aSet[Member])(cluster: Cluster) extends MemberEvent(cluster)
case class MemberAttributeUpdated(member: Member, name: String, value: Any)(cluster: Cluster) extends MemberEvent(cluster)
case class MemberAttributeRemoved(member: Member, name: String)(cluster: Cluster) extends MemberEvent(cluster)
