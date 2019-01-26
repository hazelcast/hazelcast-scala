package com.hazelcast.Scala

import java.util.Map.Entry

import com.hazelcast.Scala.dds._
import com.hazelcast.core._
import com.hazelcast.durableexecutor.DurableExecutorService
import com.hazelcast.query._

import language.implicitConversions

trait LowPriorityImplicits {

  @inline implicit def builder2anypred(p: PredicateBuilder): Predicate[Any, Any] = p.asInstanceOf[Predicate[Any, Any]]
  @inline implicit def sql2anypred(p: SqlPredicate): Predicate[Any, Any] = p.asInstanceOf[Predicate[Any, Any]]

  @inline implicit def dds2aggrDds[E](dds: DDS[E]): AggrDDS[E] = dds match {
    case dds: MapDDS[_, _, E] => new AggrMapDDS(dds)
  }
  @inline implicit def sortdds2aggrDds[E](dds: SortDDS[E]): AggrDDS[E] = dds match {
    case dds: MapSortDDS[_, _, E] => new AggrMapDDS(dds.dds, Sorted(dds.ord, dds.skip, dds.limit))
  }
  @inline implicit def dds2AggrGrpDds[G, E](dds: GroupDDS[G, E]): AggrGroupDDS[G, E] = dds match {
    case dds: MapGroupDDS[_, _, G, E] => new AggrGroupMapDDS(dds.dds)
  }
}
trait MediumPriorityImplicits extends LowPriorityImplicits {
  @inline implicit def dds2ordDds[O: Ordering](dds: DDS[O]): OrderingDDS[O] = dds match {
    case dds: MapDDS[_, _, O] => new OrderingMapDDS(dds)
  }
  @inline implicit def sortdds2ordDds[O: Ordering](dds: SortDDS[O]): OrderingDDS[O] = dds match {
    case dds: MapSortDDS[_, _, O] => new OrderingMapDDS(dds.dds, Sorted(dds.ord, dds.skip, dds.limit))
  }
  @inline implicit def dds2OrdGrpDds[G, O: Ordering](dds: GroupDDS[G, O]): OrderingGroupDDS[G, O] = dds match {
    case dds: MapGroupDDS[_, _, G, O] => new OrderingGroupMapDDS(dds.dds)
  }
}
trait HighPriorityImplicits extends MediumPriorityImplicits {
  @inline implicit def imap2dds[K, V](imap: IMap[K, V]): DDS[Entry[K, V]] = new MapDDS(imap)
  @inline implicit def imap2aggrDds[K, V](imap: IMap[K, V]): AggrDDS[Entry[K, V]] = dds2aggrDds(new MapDDS(imap))
  @inline implicit def inst2scala(inst: HazelcastInstance) = new HzHazelcastInstance(inst)
  @inline implicit def cluster2scala(cl: Cluster) = new HzCluster(cl)
  @inline implicit def clientsvc2scala(cs: ClientService) = new HzClientService(cs)
  @inline implicit def partsvc2scala(ps: PartitionService) = new HzPartitionService(ps)
  @inline implicit def topic2scala[T](topic: ITopic[T]) = new HzTopic(topic)
  @inline implicit def queue2scala[T](queue: BaseQueue[T]) = new HzQueue(queue)
  @inline implicit def txqueue2scala[T](queue: TransactionalQueue[T]) = new HzTxQueue(queue)
  @inline implicit def exec2scala(exec: IExecutorService) = new HzExecutorService(exec)
  @inline implicit def durexec2scala(exec: DurableExecutorService) = new HzDurableExecutorService(exec)
  @inline implicit def dds2numDds[N: Numeric](dds: DDS[N]): NumericDDS[N] = dds match {
    case dds: MapDDS[_, _, N] => new NumericMapDDS(dds)
  }
  @inline implicit def sortdds2numDds[N: Numeric](dds: SortDDS[N]): NumericDDS[N] = dds match {
    case dds: MapSortDDS[_, _, N] => new NumericMapDDS(dds.dds, Sorted(dds.ord, dds.skip, dds.limit))
  }
  @inline implicit def dds2NumGrpDds[G, N: Numeric](dds: GroupDDS[G, N]): NumericGroupDDS[G, N] = dds match {
    case grpDDS: MapGroupDDS[_, _, G, N] => new NumericGroupMapDDS(grpDDS.dds)
  }
  @inline implicit def dds2entryDds[K, V](dds: DDS[Entry[K, V]]): EntryMapDDS[K, V] = dds match {
    case dds: MapDDS[K, V, Entry[K, V]] @unchecked => new EntryMapDDS(dds)
  }
  @inline implicit def imap2entryDds[K, V](imap: IMap[K, V]): EntryMapDDS[K, V] = new EntryMapDDS(new MapDDS(imap))
}
