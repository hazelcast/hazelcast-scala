package com.hazelcast.Scala.dds

import scala.concurrent._
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import scala.reflect.ClassTag
import collection.{ Map => cMap }
import com.hazelcast.core._
import com.hazelcast.query._
import collection.JavaConverters._
import java.util.concurrent.{ Future => jFuture }
import language.existentials
import java.util.Map.Entry

private[Scala] class AggrMapDDS[K, E](val dds: MapDDS[K, _, E], sorted: Option[Sorted[E]] = None) extends AggrDDS[E] {

  def this(dds: MapDDS[K, _, E], sorted: Sorted[E]) = this(dds, Option(sorted))

  final def submit[R](
    aggregator: Aggregator[E, R],
    es: IExecutorService,
    ts: UserContext.Key[collection.parallel.TaskSupport])(implicit ec: ExecutionContext): Future[R] = {

    val hz = dds.imap.getHZ
    val keysByMember = dds.keySet.map(hz.groupByMember)
    val exec = if (es == null) hz.queryPool else es
    val taskSupport = Option(ts)

    sorted match {
      case None =>
        AggrMapDDS.aggregate[K, E, R, aggregator.W](dds.imap.getName, keysByMember, dds.predicate, dds.pipe, exec, aggregator, taskSupport)
      case Some(sorted) =>
        aggregator match {
          case _: Values.Complete[_] =>
            val fetch = aggr.Values(sorted)
            AggrMapDDS.aggregate(dds.imap.getName, keysByMember, dds.predicate, dds.pipe, exec, fetch, taskSupport)
          case _ =>
            val adapter = aggr.Values.Adapter(aggregator, sorted)
            AggrMapDDS.aggregate[K, E, R, adapter.W](dds.imap.getName, keysByMember, dds.predicate, dds.pipe, exec, adapter, taskSupport)
        }
    }
  }

}

private[Scala] class AggrGroupMapDDS[G, E](dds: MapDDS[_, _, (G, E)]) extends AggrGroupDDS[G, E] {
  def submitGrouped[AR, GR](
    aggr: Aggregator.Grouped[G, E, AR, GR],
    es: IExecutorService,
    ts: UserContext.Key[collection.parallel.TaskSupport])(implicit ec: ExecutionContext): Future[cMap[G, GR]] =
      dds.submit(aggr, es, ts)
}

private[Scala] class OrderingMapDDS[K, O: Ordering](
  dds: MapDDS[K, _, O], sorted: Option[Sorted[O]] = None)
    extends AggrMapDDS(dds, sorted) with OrderingDDS[O] {
  def this(dds: MapDDS[K, _, O], sorted: Sorted[O]) = this(dds, Option(sorted))
  final protected def ord = implicitly[Ordering[O]]
}
private[Scala] class OrderingGroupMapDDS[G, O: Ordering](dds: MapDDS[_, _, (G, O)])
    extends AggrGroupMapDDS(dds) with OrderingGroupDDS[G, O] {
  final protected def ord = implicitly[Ordering[O]]
}

private[Scala] class NumericMapDDS[K, N: Numeric](
  dds: MapDDS[K, _, N], sorted: Option[Sorted[N]] = None)
    extends OrderingMapDDS(dds, sorted) with NumericDDS[N] {
  def this(dds: MapDDS[K, _, N], sorted: Sorted[N]) = this(dds, Option(sorted))
  final protected def num = implicitly[Numeric[N]]
}
private[Scala] class NumericGroupMapDDS[G, N: Numeric](dds: MapDDS[_, _, (G, N)])
    extends OrderingGroupMapDDS(dds) with NumericGroupDDS[G, N] {
  final protected def num = implicitly[Numeric[N]]
}

private[Scala] final class AggrMapDDSTask[K, E, AW](
  val aggr: Aggregator[E, _] { type W = AW },
  val taskSupport: Option[UserContext.Key[collection.parallel.TaskSupport]],
  val mapName: String,
  val keysByMemberId: Map[String, collection.Set[K]],
  val predicate: Option[Predicate[_, _]],
  val pipe: Pipe[E])
    extends (HazelcastInstance => AW) with Serializable {

  import collection.JavaConverters._

  def apply(hz: HazelcastInstance): AW = {
    aggr match {
      case aggr: HazelcastInstanceAware => aggr.setHazelcastInstance(hz)
      case _ => // Ignore
    }
    val folded = processLocalData(hz)
    aggr.remoteFinalize(folded)
  }
  private def processLocalData(hz: HazelcastInstance): aggr.Q = {
    val imap = hz.getMap[K, Any](mapName)
    val (localKeys, includeEntry) = keysByMemberId.get(hz.getCluster.getLocalMember.getUuid) match {
      case None =>
        assert(keysByMemberId.isEmpty) // If keys are known, this code should not be running on this member
        predicate.map(imap.localKeySet(_)).getOrElse(imap.localKeySet).asScala -> TruePredicate.INSTANCE.asInstanceOf[Predicate[K, Any]]
      case Some(keys) =>
        keys -> predicate.getOrElse(TruePredicate.INSTANCE).asInstanceOf[Predicate[K, Any]]
    }
    if (localKeys.isEmpty) aggr.remoteInit
    else {
      type Q = aggr.Q
      val remoteFold = aggr.remoteFold _
      val entryFold = pipe.prepare[Q](hz)
      val seqop = (acc: Q, entry: Entry[K, Any]) => {
        if (includeEntry(entry)) {
          entryFold.foldEntry(acc, entry)(remoteFold)
        } else acc
      }
      val partSvc = hz.getPartitionService
      val keysByPartId = localKeys.groupBy(partSvc.getPartition(_).getPartitionId).values.par
      taskSupport.foreach { taskSupport =>
        keysByPartId.tasksupport = hz.userCtx(taskSupport)
      }
      val entries = keysByPartId.map(parKeys => blocking(imap.getAll(parKeys.asJava))).flatMap(_.entrySet.asScala)
      entries.aggregate(aggr.remoteInit)(seqop, aggr.remoteCombine)
    }
  }
}

private object AggrMapDDS {

  type OptionalTaskSupport = Option[UserContext.Key[collection.parallel.TaskSupport]]

  private def aggregate[K, E, R, AW](
    mapName: String,
    keysByMember: Option[Map[Member, collection.Set[K]]],
    predicate: Option[Predicate[_, _]],
    pipe: Option[Pipe[E]],
    es: IExecutorService,
    aggr: Aggregator[E, R] { type W = AW },
    taskSupport: OptionalTaskSupport)(implicit ec: ExecutionContext): Future[R] = {

    val (keysByMemberId, submitTo) = keysByMember match {
      case None => Map.empty[String, Set[K]] -> ToAll
      case Some(keysByMember) =>
        val keysByMemberId = keysByMember.map {
          case (member, keys) => member.getUuid -> keys
        }
        keysByMemberId -> ToMembers(keysByMember.keys)
    }
    val values = submitFold(es, submitTo, mapName, keysByMemberId, predicate, pipe getOrElse PassThroughPipe[E], aggr, taskSupport)
    val reduced = Future.reduce(values)(aggr.localCombine)
    reduced.map(aggr.localFinalize(_))(SameThread)
  }
  private def submitFold[K, E](
    es: IExecutorService,
    submitTo: MultipleMembers,
    mapName: String,
    keysByMemberId: Map[String, collection.Set[K]],
    predicate: Option[Predicate[_, _]],
    pipe: Pipe[E],
    aggr: Aggregator[E, _],
    taskSupport: OptionalTaskSupport): Iterable[Future[aggr.W]] = {

    val remoteTask = new AggrMapDDSTask[K, E, aggr.W](aggr, taskSupport, mapName, keysByMemberId, predicate, pipe)
    es.submit(submitTo)(remoteTask).values
  }

}
