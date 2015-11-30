package com.hazelcast.Scala.dds

import com.hazelcast.core.IMap
import java.util.Map.Entry
import collection.JavaConverters._
import collection.mutable.{ Map => mMap }
import com.hazelcast.query.Predicate
import com.hazelcast.core._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag
import scala.collection.immutable.{ Set, SortedMap, TreeMap }
import scala.collection.mutable.{ Map => mMap }

import com.hazelcast.Scala._

/** Distributed data structure. */
sealed trait DDS[E] {
  private[Scala] def hz: HazelcastInstance
  private[Scala] def name: String

  def filter(f: E => Boolean)(implicit classTag: ClassTag[E]): DDS[E]
  def map[F](m: E => F): DDS[F]
  def collect[F](pf: PartialFunction[E, F]): DDS[F]
  def flatMap[F](fm: E => Traversable[F]): DDS[F]

  final def groupBy[G](gf: E => G): GroupDDS[G, E] = groupBy[G, E](gf, identity)
  def groupBy[G, F](gf: E => G, mf: E => F): GroupDDS[G, F]

  //  def innerJoinOne[JK, JV](join: IMap[JK, JV], on: E => Option[JK]): DDS[(E, JV)]
  //  def innerJoinMany[JK, JV](join: IMap[JK, JV], on: E => Set[JK]): DDS[(E, Map[JK, JV])]
  //  def outerJoinOne[JK, JV](join: IMap[JK, JV], on: E => Option[JK]): DDS[(E, Option[JV])]
  //  def outerJoinMany[JK, JV](join: IMap[JK, JV], on: E => Set[JK]): DDS[(E, Map[JK, JV])]
}

sealed trait GroupDDS[G, E]
private[Scala] final class MapGroupDDS[K, V, G, E](val dds: MapDDS[K, V, (G, E)]) extends GroupDDS[G, E]

private[Scala] final class MapDDS[K, V, E](
    val hz: HazelcastInstance,
    val name: String,
    val predicate: Option[Predicate[_, _]],
    val keySet: Option[collection.Set[K]],
    val pipe: Option[Pipe[E]]) extends DDS[E] {

  private[Scala] def this(imap: IMap[K, V], predicate: Predicate[_, _] = null) = this(imap.getHZ, imap.getName, Option(predicate), None, None)

  def groupBy[G, F](gf: E => G, mf: E => F): GroupDDS[G, F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new GroupByPipe(gf, mf, prev)
    new MapGroupDDS[K, V, G, F](new MapDDS(this.hz, this.name, this.predicate, this.keySet, Some(pipe)))
  }

  def filter(f: E => Boolean)(implicit classTag: ClassTag[E]): DDS[E] = {
    if (this.pipe.isEmpty && classOf[Entry[_, _]].isAssignableFrom(classTag.runtimeClass)) {
      val filter = f.asInstanceOf[Entry[_, _] => Boolean]
      val predicate = new ScalaEntryPredicate(filter, this.predicate.orNull.asInstanceOf[Predicate[Object, Object]])
      new MapDDS[K, V, E](this.hz, this.name, Some(predicate), this.keySet, this.pipe)
    } else {
      val prev = this.pipe getOrElse PassThroughPipe[E]
      val pipe = new FilterPipe(f, prev)
      new MapDDS[K, V, E](this.hz, this.name, this.predicate, this.keySet, Some(pipe))
    }
  }
  def map[F](mf: E => F): DDS[F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new MapPipe(mf, prev)
    new MapDDS[K, V, F](hz, name, predicate, keySet, Some(pipe))
  }
  def collect[F](pf: PartialFunction[E, F]): DDS[F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new CollectPipe(pf, prev)
    new MapDDS[K, V, F](hz, name, predicate, keySet, Some(pipe))
  }
  def flatMap[F](fm: E => Traversable[F]): DDS[F] = {
    val prev = this.pipe getOrElse PassThroughPipe[E]
    val pipe = new FlatMapPipe[E, F](fm, prev)
    new MapDDS[K, V, F](hz, name, predicate, keySet, Some(pipe))
  }

  //  private def withJoin[J <: Join[E, _, _]](join: J): DDS[(E, J#T)] = {
  //    val prevPipe = this.pipe getOrElse PassThroughPipe[E]
  //    val pipe = new JoinPipe[E, (E, J#T)](join, prevPipe)
  //    new MapDDS[K, V, (E, J#T)](hz, name, predicate, keySet, Some(pipe))
  //  }
  //  def innerJoinOne[JK, JV](join: IMap[JK, JV], on: E => Option[JK]): DDS[(E, JV)] = withJoin(InnerOne[E, JK, JV](join.getName, on))
  //  def innerJoinMany[JK, JV](join: IMap[JK, JV], on: E => Set[JK]): DDS[(E, Map[JK, JV])]
  //  def outerJoinOne[JK, JV](join: IMap[JK, JV], on: E => Option[JK]): DDS[(E, Option[JV])]
  //  def outerJoinMany[JK, JV](join: IMap[JK, JV], on: E => Set[JK]): DDS[(E, Map[JK, JV])]

}
