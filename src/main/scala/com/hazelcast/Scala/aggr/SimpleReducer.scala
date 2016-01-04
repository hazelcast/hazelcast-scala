package com.hazelcast.Scala.aggr

trait SimpleReducer[T] extends AbstractReducer[T] {
  type R = T
  final def localFinalize(t: T): T = t
}
