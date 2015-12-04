package com.hazelcast.Scala

trait Aggregation[Q, -T, W, +R] extends Serializable {
  def remoteInit: Q
  def remoteFold(q: Q, t: T): Q
  def remoteCombine(x: Q, y: Q): Q
  def remoteFinalize(q: Q): W

  def localCombine(x: W, y: W): W
  def localFinalize(w: W): R
}

object Aggregation {
  trait FinalizeSimple[T, R] extends Aggregation[T, T, T, R] {
    def init: T
    def reduce(x: T, y: T): T
    final def remoteInit = init
    final def remoteFold(a: T, t: T): T = reduce(a, t)
    final def remoteCombine(x: T, y: T): T = reduce(x, y)
    final def remoteFinalize(t: T): T = t
    final def localCombine(x: T, y: T): T = reduce(x, y)
  }
  trait Simple[T] extends FinalizeSimple[T, T] {
    final def localFinalize(t: T): T = t
  }

  type JM[G, V] = java.util.HashMap[G, V]
  type SM[G, V] = collection.mutable.Map[G, V]

  def groupAll[G, Q, T, W, R](aggr: Aggregation[Q, T, W, R]) = new GroupAggregation[G, Q, T, W, R, R](aggr, (_: R) => true, (r: R) => r)
  def groupSome[G, Q, T, W, R](aggr: Aggregation[Q, T, W, Option[R]]) = new GroupAggregation[G, Q, T, W, Option[R], R](aggr, (opt: Option[R]) => opt.isDefined, (opt: Option[R]) => opt.get)

  class GroupAggregation[G, Q, T, W, AR, GR](
    aggr: Aggregation[Q, T, W, AR],
    includeResult: AR => Boolean, mapResult: AR => GR)
      extends Aggregation[JM[G, Q], (G, T), JM[G, W], SM[G, GR]] {

    import collection.JavaConverters._

    type Map[V] = JM[G, V]
    type Tuple = (G, T)

    private def combine[V](jx: Map[V], jy: Map[V])(comb: (V, V) => V) = {
      val sy = jy.asScala
      val (intersection, yOnly) = sy.partition(e => jx.containsKey(e._1))
      val combined = intersection.foldLeft(jx) {
        case (map, entry) =>
          map.put(entry._1, comb(map.get(entry._1), entry._2))
          map
      }
      combined.putAll(yOnly.asJava)
      combined
    }
    protected def finalize[A, B](map: Map[A])(fin: A => B): Map[B] = {
      val iter = map.asInstanceOf[Map[Any]].entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next
        fin(entry.value.asInstanceOf[A]) match {
          case null =>
            iter.remove()
          case finalized =>
            entry.value = finalized
        }
      }
      map.asInstanceOf[Map[B]]
    }

    def remoteInit = new Map[Q](64)
    def remoteFold(map: Map[Q], t: Tuple): Map[Q] = {
      val acc = map.get(t._1) match {
        case null => aggr.remoteInit
        case acc => acc
      }
      map.put(t._1, aggr.remoteFold(acc, t._2))
      map
    }
    def remoteCombine(x: Map[Q], y: Map[Q]): Map[Q] = combine(x, y)(aggr.remoteCombine)
    def remoteFinalize(q: Map[Q]): Map[W] = finalize(q)(aggr.remoteFinalize)
    def localCombine(x: Map[W], y: Map[W]): Map[W] = combine(x, y)(aggr.localCombine)
    def localFinalize(w: Map[W]): SM[G, GR] = {
      val finalized = finalize(w) { w =>
        val ar = aggr.localFinalize(w)
        if (includeResult(ar)) mapResult(ar)
        else null.asInstanceOf[GR]
      }
      finalized.asScala
    }
  }

}
