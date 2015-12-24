package com.hazelcast.Scala

/**
 * Generalized aggregation interface.
 * @tparam Q Accumulator type
 * @tparam T DDS type
 * @tparam W Accumulation wire format
 */
trait Aggregator[Q, -T, W] extends Serializable {
  /** Final result type. */
  type R

  // `remote` functions are executed locally
  // on the nodes where the data resides, but
  // remotely seen from the perspective of the
  // node or client that submits the aggregation

  def remoteInit: Q
  def remoteFold(q: Q, t: T): Q
  def remoteCombine(x: Q, y: Q): Q
  def remoteFinalize(q: Q): W

  // `local` functions are executed locally
  // on the node or client that submits the
  // aggregation

  def localCombine(x: W, y: W): W
  def localFinalize(w: W): R

}

object Aggregator {

  type JM[G, V] = java.util.HashMap[G, V]
  type SM[G, V] = collection.mutable.Map[G, V]

  /** Group into `Map` according to Aggregator result type. */
  def groupAll[G, Q, T, W, AR](aggr: Aggregator[Q, T, W] { type R = AR }) =
    new Grouped[G, Q, T, W, AR, AR](aggr, PartialFunction(identity))
  /** Group into `Map` according to Aggregator result type inside `Option`. */
  def groupSome[G, Q, T, W, AR](aggr: Aggregator[Q, T, W] { type R = Option[AR] }) =
    new Grouped[G, Q, T, W, Option[AR], AR](aggr, {
      case Some(value) => value
    })

  class Grouped[G, Q, T, W, AR, GR](
    aggr: Aggregator[Q, T, W] { type R = AR }, pf: PartialFunction[AR, GR])
      extends Aggregator[JM[G, Q], (G, T), JM[G, W]] {

    import collection.JavaConverters._
    type R = SM[G, GR]
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
    private def finalize[A, B](map: Map[A])(fin: A => B): Map[B] = {
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
    def localFinalize(w: Map[W]): R = {
      val finalized = finalize(w) { w =>
        val ar = aggr.localFinalize(w)
        if (pf.isDefinedAt(ar)) pf(ar)
        else null.asInstanceOf[GR]
      }
      finalized.asScala
    }
  }

}
