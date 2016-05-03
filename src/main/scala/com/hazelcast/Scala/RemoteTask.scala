package com.hazelcast.Scala

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceAware
import scala.beans.BeanProperty
import java.util.concurrent.Callable

private[Scala] final class Task[T](val thunk: () => T)
    extends Callable[T] with Serializable {
  def call(): T = thunk()
}

private[Scala] final class InstanceAwareTask[T](val thunk: HazelcastInstance => T)
    extends Callable[T] with Serializable with HazelcastInstanceAware {
  @BeanProperty @transient
  var hazelcastInstance: HazelcastInstance = _
  def call(): T = thunk(hazelcastInstance)
}
