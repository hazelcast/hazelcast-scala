package com.hazelcast.Scala

/**
  * Generalized aggregation interface.
  * @tparam T DDS type
  * @tparam R Final result type
  */
trait Aggregator[-T, +R] extends Serializable {
  /** Accumulator type. */
  type Q
  /** Accumulator serialization type. */
  type W

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

  /**
    *  User context key for overriding the
    *  default `TaskSupport` for parallel collections.
    *
    *  Example:
    *  {{{
    *  hz.userCtx(Aggregator.TaskSupport) = new ForkJoinTaskSupport()
    *  }}}
    */
  object TaskSupport extends UserContext.Key[collection.parallel.TaskSupport]

  type JM[G, V] = java.util.HashMap[G, V]
  type SM[G, V] = collection.mutable.Map[G, V]

  /** Group into `Map` according to Aggregator result type. */
  def groupAll[G, T, R](aggr: Aggregator[T, R]) =
    new Grouped[G, T, R, R](aggr, PartialFunction(identity))
  /** Group into `Map` according to Aggregator result type inside `Option`. */
  def groupSome[G, T, R](aggr: Aggregator[T, Option[R]]) =
    new Grouped[G, T, Option[R], R](aggr, {
      case Some(value) => value
    })

  class Grouped[G, T, R, GR](
    val aggr: Aggregator[T, R], val pf: PartialFunction[R, GR])
      extends Aggregator[(G, T), SM[G, GR]] {

    import collection.JavaConverters._
    type AQ = aggr.Q
    type AW = aggr.W
    type Q = JM[G, AQ]
    type W = JM[G, AW]
    type Map[V] = JM[G, V]
    type Tuple = (G, T)

    private def combine[V](jx: Map[V], jy: Map[V])(comb: (V, V) => V) = {
      val fold =
        if (jx.size < jy.size) {
          jx.entrySet.iterator.asScala.foldLeft(jy) _
        } else {
          jy.entrySet.iterator.asScala.foldLeft(jx) _
        }
      fold {
        case (jmap, entry) =>
          jmap.put(entry.key, entry.value) match {
            case null => // Ok, no previous value
            case prev =>
              jmap.put(entry.key, comb(prev, entry.value))
          }
          jmap
      }
    }
    private def finalize[A, B](map: Map[A])(fin: A => B): Map[B] = {
      val iter = map.asInstanceOf[Map[Any]].entrySet.iterator
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

    def remoteInit = new Map[AQ](64)
    def remoteFold(map: Map[AQ], t: Tuple): Map[AQ] = {
      val acc = map.get(t._1) match {
        case null => aggr.remoteInit
        case acc => acc
      }
      map.put(t._1, aggr.remoteFold(acc, t._2))
      map
    }
    def remoteCombine(x: Map[AQ], y: Map[AQ]): Map[AQ] = combine(x, y)(aggr.remoteCombine)
    def remoteFinalize(q: Map[AQ]): Map[AW] = finalize(q)(aggr.remoteFinalize)
    def localCombine(x: Map[AW], y: Map[AW]): Map[AW] = combine(x, y)(aggr.localCombine)
    def localFinalize(w: Map[AW]): SM[G, GR] = {
      val finalized = finalize(w) { w =>
        val ar = aggr.localFinalize(w)
        if (pf.isDefinedAt(ar)) pf(ar)
        else null.asInstanceOf[GR]
      }
      finalized.asScala
    }
  }

}
