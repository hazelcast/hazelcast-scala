package com.hazelcast.Scala.actress

import scala.concurrent.Future
import com.hazelcast.core.HazelcastInstance

trait ActressRef[A <: AnyRef] {
  def name: String
  def apply[R](thunk: (HazelcastInstance, A) => R): Future[R]
}
