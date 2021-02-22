package com.hazelcast.Scala

import com.hazelcast.cluster.{Member, MemberSelector}
import java.util.concurrent.Callable
import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import com.hazelcast.core.{HazelcastInstance, IExecutorService}
import com.hazelcast.durableexecutor.DurableExecutorService

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
        jResult.asScala.view.mapValues(_.asScala).toMap
      case ToMembers(members) =>
        members.foldLeft(Map.empty[Member, Future[T]]) {
          case (map, member) =>
            val callback = new FutureCallback[T, T]()
            exec.submitToMember(task, member, callback)
            map.updated(member, callback.future)
        }
      case selector: ToMembersWhere =>
        val fMap = exec.submitToMembers(task, selector)
        fMap.entrySet.iterator.asScala.foldLeft(Map.empty[Member, Future[T]]) {
          case (map, entry) =>
            val future = entry.value.asScala
            map.updated(entry.key, future)
        }
    }
  }

}

final class HzDurableExecutorService(private val exec: DurableExecutorService) extends AnyVal {
  import scala.jdk.FutureConverters._

  def retrieveAndDispose[T](taskId: Long): Future[T] =
    exec.retrieveAndDisposeResult(taskId).asScala
  def retrieve[T](taskId: Long): Future[T] =
    exec.retrieveResult(taskId).asScala

  def submit[T]()(thunk: HazelcastInstance => T): (Future[T], Long) = {
    val future = exec.submit(new RemoteTask(thunk))
    future.asScala -> future.getTaskId
  }
  def submit[T](member: ToKeyOwner)(thunk: HazelcastInstance => T): (Future[T], Long) = {
    val future = exec.submitToKeyOwner(new RemoteTask(thunk), member.key)
    future.asScala -> future.getTaskId
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
final case class ToMembers(members: Iterable[Member]) extends MultipleMembers
object ToMembers {
  def apply(members: Member*): ToMembers = ToMembers(members: _*)
}
final case class ToMembersWhere(filter: Member => Boolean) extends MultipleMembers with MemberSelector {
  def select(member: Member) = filter(member)
}
