package com.hazelcast.Scala.aggr

trait SimpleReducer[T] extends AbstractReducer[T, T] {
  final def localFinalize(t: T): T = t
}
