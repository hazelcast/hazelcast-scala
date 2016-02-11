package com.hazelcast.Scala.dds

import com.hazelcast.core.IMap
import java.util.Map.Entry
import collection.JavaConverters._
import com.hazelcast.query.Predicate
import com.hazelcast.core._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag
import scala.collection.{ Set => aSet }

import com.hazelcast.Scala._

/** Distributed data structure. */
sealed trait DDS[E] {

  def filter(f: E => Boolean): DDS[E]
  def map[F](m: E => F): DDS[F]
  def collect[F: ClassTag](pf: PartialFunction[E, F]): DDS[F]
  def flatMap[F: ClassTag](fm: E => Traversable[F]): DDS[F]

  final def groupBy(): GroupDDS[E, E] = groupBy[E, E](identity, identity)
  final def groupBy[G](gf: E => G): GroupDDS[G, E] = groupBy[G, E](gf, identity)
  def groupBy[G, F](gf: E => G, mf: E => F): GroupDDS[G, F]

  def sortBy[S: Ordering](sf: E => S): SortDDS[E]
  final def sorted()(implicit ord: Ordering[E]): SortDDS[E] = sortBy(identity)

  def innerJoinOne[JK, JV](join: IMap[JK, JV], on: E => JK): DDS[(E, JV)]
  def innerJoinMany[JK, JV](join: IMap[JK, JV], on: E => aSet[JK]): DDS[(E, collection.Map[JK, JV])]
  def outerJoinOne[JK, JV](join: IMap[JK, JV], on: E => JK): DDS[(E, Option[JV])]
  def outerJoinMany[JK, JV](join: IMap[JK, JV], on: E => aSet[JK]): DDS[(E, collection.Map[JK, JV])]
}

sealed trait SortDDS[E] {
  def drop(count: Int): SortDDS[E]
  def take(count: Int): SortDDS[E]
  def reverse(): SortDDS[E]
}
private[Scala] final class MapSortDDS[K, V, E](
  val dds: MapDDS[K, V, E], val ord: Ordering[E], val skip: Int = 0, val limit: Option[Int] = None)
    extends SortDDS[E] {

  def drop(count: Int): SortDDS[E] = {
    val skip = 0 max count
    limit match {
      case None => new MapSortDDS(dds, ord, this.skip + skip, limit)
      case _ => this
    }
  }
  def take(count: Int): SortDDS[E] = {
    val limit = 0 max count
    val someLimit = this.limit.map(_ min limit) orElse Some(limit)
    new MapSortDDS(dds, ord, skip, someLimit)
  }
  def reverse(): SortDDS[E] = new MapSortDDS(dds, ord.reverse, skip, limit)
}

sealed trait GroupDDS[G, E]
private[Scala] final class MapGroupDDS[K, V, G, E](val dds: MapDDS[K, V, (G, E)])
  extends GroupDDS[G, E]

private[Scala] final class MapDDS[K, V, E](
    val imap: IMap[K, V],
    val predicate: Option[Predicate[_, _]],
    val keySet: Option[collection.Set[K]],
    val pipe: Option[Pipe[E]]) extends DDS[E] {

  private[Scala] def this(imap: IMap[K, V], predicate: Predicate[_, _] = null) = this(imap, Option(predicate), None, None)

  def groupBy[G, F](gf: E => G, mf: E => F): GroupDDS[G, F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new GroupByPipe(gf, mf, prev)
    new MapGroupDDS[K, V, G, F](new MapDDS(this.imap, this.predicate, this.keySet, Some(pipe)))
  }

  def filter(f: E => Boolean): DDS[E] = {
    if (this.pipe.isEmpty) {
      val filter = f.asInstanceOf[Entry[_, _] => Boolean]
      val predicate = new EntryPredicate(filter, this.predicate.orNull.asInstanceOf[Predicate[Object, Object]])
      new MapDDS[K, V, E](this.imap, Some(predicate), this.keySet, this.pipe)
    } else {
      val prev = this.pipe getOrElse PassThroughPipe[E]
      val pipe = new FilterPipe(f, prev)
      new MapDDS[K, V, E](this.imap, this.predicate, this.keySet, Some(pipe))
    }
  }
  def map[F](mf: E => F): DDS[F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new MapPipe(mf, prev)
    new MapDDS[K, V, F](imap, predicate, keySet, Some(pipe))
  }
  def collect[F: ClassTag](pf: PartialFunction[E, F]): DDS[F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new CollectPipe(pf, prev)
    new MapDDS[K, V, F](imap, predicate, keySet, Some(pipe))
  }
  def flatMap[F: ClassTag](fm: E => Traversable[F]): DDS[F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new FlatMapPipe[E, F](fm, prev)
    new MapDDS[K, V, F](imap, predicate, keySet, Some(pipe))
  }

  def sortBy[S: Ordering](sf: E => S): SortDDS[E] = {
    val ord = implicitly[Ordering[S]].on(sf)
    new MapSortDDS(this, ord)
  }

  private def withJoin[JT](join: Join[E, _, _] { type T = (E, JT) }): DDS[(E, JT)] = {
    val prevPipe = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new JoinPipe[E, (E, JT)](join, prevPipe)
    new MapDDS[K, V, (E, JT)](imap, predicate, keySet, Some(pipe))
  }
  def innerJoinOne[JK, JV](join: IMap[JK, JV], on: E => JK): DDS[(E, JV)] = withJoin(InnerOne(join.getName, on))
  def innerJoinMany[JK, JV](join: IMap[JK, JV], on: E => aSet[JK]): DDS[(E, collection.Map[JK, JV])] = withJoin(InnerMany(join.getName, on))
  def outerJoinOne[JK, JV](join: IMap[JK, JV], on: E => JK): DDS[(E, Option[JV])] = withJoin(OuterOne(join.getName, on))
  def outerJoinMany[JK, JV](join: IMap[JK, JV], on: E => aSet[JK]): DDS[(E, collection.Map[JK, JV])] = withJoin(OuterMany(join.getName, on))

}
