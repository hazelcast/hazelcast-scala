package com.hazelcast.Scala

import com.hazelcast.core.IExecutorService
import scala.concurrent.Future
import scala.concurrent.Promise
import com.hazelcast.core.ExecutionCallback
import java.util.concurrent.Callable
import com.hazelcast.core.Member
import scala.reflect.{ ClassTag, classTag }
import com.hazelcast.core.MemberSelector
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceAware
import scala.beans.BeanProperty
import com.hazelcast.core.ICompletableFuture
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import com.hazelcast.instance.Node
import com.hazelcast.spi.NodeAware
import collection.JavaConversions._
import com.hazelcast.core.PartitionAware

final class HzExecutorService(private val exec: IExecutorService) extends AnyVal {

  private def submitSingle[F, T](toMember: SingleMember, task: Callable[T]): Future[T] = {
    val callback = new FutureCallback[T, T]()
    toMember match {
      case ToOne =>
        exec.submit(task, callback)
      case ToKeyOwner(key) =>
        exec.submitToKeyOwner(task, key, callback)
      case ToMember(member) =>
        exec.submitToMember(task, member, callback)
      case ToOneWhere(selector) =>
        exec.submit(task, selector, callback)
      case ToLocal =>
        exec.submit(task, ToLocal.selector, callback)
    }
    callback.future
  }

  def submit[T](toMember: SingleMember = ToOne)(thunk: HazelcastInstance => T): Future[T] =
    submitSingle(toMember, new RemoteTask(thunk))

  def submit[T](toMembers: MultipleMembers)(thunk: HazelcastInstance => T): Map[Member, Future[T]] =
    submitMultiple(toMembers, new RemoteTask(thunk))

  private def submitMultiple[T](toMembers: MultipleMembers, task: Callable[T]): Map[Member, Future[T]] = {
    toMembers match {
      case ToAll =>
        val jResult = exec.submitToAllMembers(task)
        jResult.mapValues(_.asScala).toMap
      case ToMembers(members) =>
        members.foldLeft(Map.empty[Member, Future[T]]) {
          case (map, member) =>
            val callback = new FutureCallback[T, T]()
            exec.submitToMember(task, member, callback)
            map.updated(member, callback.future)
        }
      case selector: ToMembersWhere =>
        val fMap = exec.submitToMembers(task, selector)
        fMap.entrySet().foldLeft(Map.empty[Member, Future[T]]) {
          case (map, entry) =>
            val future = entry.value.asScala
            map.updated(entry.key, future)
        }
    }
  }

}

sealed trait SingleMember
final case object ToOne extends SingleMember
final case object ToLocal extends SingleMember {
  private[Scala] val selector = new MemberSelector {
    def select(member: Member) = member.localMember
  }
}
final case class ToOneWhere(selector: MemberSelector) extends SingleMember
object ToOneWhere {
  def apply(filter: Member => Boolean) = {
    val selector = new MemberSelector { def select(mbr: Member) = filter(mbr) }
    new ToOneWhere(selector)
  }
}
final case class ToKeyOwner(key: Any) extends SingleMember
final case class ToMember(member: Member) extends SingleMember

sealed trait MultipleMembers
final case object ToAll extends MultipleMembers
final case class ToMembers(members: Traversable[Member]) extends MultipleMembers
object ToMembers {
  def apply(members: Member*): ToMembers = ToMembers(members: _*)
}
final case class ToMembersWhere(filter: Member => Boolean) extends MultipleMembers with MemberSelector {
  def select(member: Member) = filter(member)
}
