package com.hazelcast.Scala

import java.util.Map.Entry
import com.hazelcast.core.HazelcastInstance
import scala.concurrent.Await

private[Scala] sealed trait EntryFold[A, +V] extends Serializable {
  def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, V) => A): A
}

private[Scala] sealed trait Pipe[+V] extends Serializable {
  def prepare[A](hz: HazelcastInstance): EntryFold[A, V]
}

private[Scala] object PassThroughPipe extends Pipe[Any] {
  def apply[E]: Pipe[E] = this.asInstanceOf[Pipe[E]]
  private[this] val passThrough = new EntryFold[Any, Any] {
    def foldEntry(acc: Any, entry: Entry[_, _])(fold: (Any, Any) => Any): Any = fold(acc, entry)
  }
  def prepare[A](hz: HazelcastInstance) = passThrough.asInstanceOf[EntryFold[A, Any]]
}
private[Scala] class MapPipe[I, V](map: I => V, prev: Pipe[I]) extends Pipe[V] {
  def prepare[A](hz: HazelcastInstance) = new EntryFold[A, V] {
    private[this] val prevFold = prev.prepare[A](hz)
    def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, V) => A): A =
      prevFold.foldEntry(acc, entry) {
        case (acc, any) => fold(acc, map(any))
      }
  }
}
private[Scala] class MapTransformPipe[EK, EV, T](transform: Entry[EK, EV] => T, prev: Pipe[Entry[EK, EV]]) extends Pipe[Entry[EK, T]] {
  def this(prev: Pipe[Entry[EK, EV]], mvf: EV => T) = this((entry: Entry[EK, EV]) => mvf(entry.value), prev)
  type V = Entry[EK, T]
  def prepare[A](hz: HazelcastInstance) = new EntryFold[A, V] {
    private[this] val prevFold = prev.prepare[A](hz)
    def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, V) => A): A =
      prevFold.foldEntry(acc, entry) {
        case (acc, entry) =>
          val tEntry = new ImmutableEntry(entry.key, transform(entry))
          fold(acc, tEntry)
      }
  }
}
private[Scala] class FilterPipe[E](include: E => Boolean, prev: Pipe[E]) extends Pipe[E] {
  def prepare[A](hz: HazelcastInstance) = new EntryFold[A, E] {
    private[this] val prevFold = prev.prepare[A](hz)
    def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, E) => A): A =
      prevFold.foldEntry(acc, entry) {
        case (acc, any) if include(any) => fold(acc, any)
        case (acc, _) => acc
      }
  }
}
private[Scala] class FlatMapPipe[E, V](flatMap: E => Traversable[V], prev: Pipe[E]) extends Pipe[V] {
  def prepare[A](hz: HazelcastInstance) = new EntryFold[A, V] {
    private[this] val prevFold = prev.prepare[A](hz)
    def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, V) => A): A =
      prevFold.foldEntry(acc, entry) {
        case (acc, any) => flatMap(any).foldLeft(acc)(fold)
      }
  }
}
private[Scala] class CollectPipe[E, V](pf: PartialFunction[E, V], prev: Pipe[E]) extends Pipe[V] {
  def prepare[A](hz: HazelcastInstance) = new EntryFold[A, V] {
    private[this] val prevFold = prev.prepare[A](hz)
    def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, V) => A): A =
      prevFold.foldEntry(acc, entry) {
        case (acc, any) if pf.isDefinedAt(any) => fold(acc, pf(any))
        case (acc, _) => acc
      }
  }
}

private[Scala] class GroupByPipe[E, G, F](gf: E => G, mf: E => F, prev: Pipe[E]) extends Pipe[(G, F)] {
  type V = (G, F)
  def prepare[A](hz: HazelcastInstance) = new EntryFold[A, V] {
    private[this] val prevFold = prev.prepare[A](hz)
    def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, V) => A): A =
      prevFold.foldEntry(acc, entry) {
        case (acc, any) => fold(acc, gf(any) -> mf(any))
      }
  }
}

//private[Scala] final class JoinPipe[E, JT](join: Join[E, _, _] { type T = JT }, prev: Pipe[E]) extends Pipe[JT] {
//  def prepare[A](hz: HazelcastInstance) = new EntryFold[A, JT] {
//    private[this] val prevFold = prev.prepare[A](hz)
//    private[this] val hzJoin = join.init(hz)
//    def foldEntry(acc: A, entry: Entry[_, _])(fold: (A, JT) => A): A = {
//      prevFold.foldEntry(acc, entry) {
//        case (acc, elem) =>
//          val result = hzJoin(elem)
//          join.callback(acc, result)(fold)
//      }
//    }
//  }
//}
