package com.hazelcast.Scala

import java.util.concurrent.Callable

import scala.beans.BeanProperty

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceAware

private[Scala] final class RemoteTask[T](val thunk: HazelcastInstance => T)
    extends Callable[T] with Serializable with HazelcastInstanceAware {
  @BeanProperty @transient
  var hazelcastInstance: HazelcastInstance = _
  def call(): T = thunk(hazelcastInstance)
}
