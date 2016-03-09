package com.hazelcast.Scala.aggr

import com.hazelcast.core.HazelcastInstanceAware
import com.hazelcast.Scala._
import com.hazelcast.core.HazelcastInstance
import collection.JavaConverters._

private[Scala] final class InlineAggregator[E, A](
  val init: () => A, _seqop: (A, E) => A, _combop: (A, A) => A)
    extends Aggregator[E, A] {
  def seqop = _seqop
  def combop = _combop
  type Q = A; type W = A
  def remoteInit: Q = init()
  def remoteFold(q: Q, e: E): Q = _seqop(q, e)
  def remoteCombine(x: Q, y: Q): Q = _combop(x, y)
  def remoteFinalize(q: Q): W = q
  def localCombine(x: W, y: W): W = _combop(x, y)
  def localFinalize(w: W): A = w
}
private[Scala] final class InlineUnitAggregator[E, A](
  val init: () => A, _seqop: (A, E) => A, _combop: (A, A) => A)
    extends Aggregator[E, Unit] {
  def seqop = _seqop
  def combop = _combop
  type Q = A; type W = Unit
  def remoteInit: Q = init()
  def remoteFold(q: Q, e: E): Q = _seqop(q, e)
  def remoteCombine(x: Q, y: Q): Q = _combop(x, y)
  def remoteFinalize(q: Q): W = ()
  def localCombine(x: W, y: W): W = ()
  def localFinalize(w: W) = ()
}

private[Scala] final class InlineSavingAggregator[E, A, AK](
  val mapName: String, val mapKey: AK,
  val init: () => A, _seqop: (A, E) => A, _combop: (A, A) => A)
    extends Aggregator[E, Unit]
    with HazelcastInstanceAware {
  @volatile @transient
  private[this] var toMap: HzMap[AK, A] = _
  def setHazelcastInstance(hz: HazelcastInstance): Unit = toMap = hz.getMap[AK, A](mapName)
  def seqop = _seqop
  def combop = _combop
  type Q = A; type W = Unit
  def remoteInit: Q = init()
  def remoteFold(q: Q, e: E): Q = _seqop(q, e)
  def remoteCombine(x: Q, y: Q): Q = _combop(x, y)
  def remoteFinalize(q: Q): W = {
    toMap.upsert(mapKey, q)(_combop(_, q))
  }
  def localCombine(x: W, y: W): W = ()
  def localFinalize(w: W) = ()
}

private[Scala] final class InlineSavingGroupAggregator[G, E, A](
  val mapName: String,
  val unitAggr: InlineUnitAggregator[E, A])
    extends Aggregator.Grouped[G, E, Unit, Unit](unitAggr, PartialFunction(identity))
    with HazelcastInstanceAware {
  def this(mapName: String, init: () => A, seqop: (A, E) => A, combop: (A, A) => A) =
    this(mapName, new InlineUnitAggregator(init, seqop, combop))
  @volatile @transient
  private[this] var toMap: HzMap[G, A] = _
  def setHazelcastInstance(hz: HazelcastInstance): Unit = toMap = hz.getMap[G, A](mapName)
  override def remoteFinalize(q: Map[AQ]): Map[AW] = {
    q.entrySet().iterator().asScala.foreach { entry =>
      val value = entry.value.asInstanceOf[A]
      entry.value = null.asInstanceOf[AQ]
      toMap.upsert(entry.getKey, value)(unitAggr.combop(_, value))
    }
    q.asInstanceOf[Map[AW]]
  }
  override def localCombine(x: W, y: W): W = {
    x.putAll(y)
    x
  }
  override def localFinalize(w: W) = w.asScala.asInstanceOf[collection.mutable.Map[G, Unit]]
}
