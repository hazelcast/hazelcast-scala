package com.hazelcast.Scala

import java.util.concurrent.Callable

import scala.beans.BeanProperty
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceAware
import com.hazelcast.nio.Address

private[Scala] final class RemoteTask[T](val thunk: (HazelcastInstance, Option[Address]) => T)
    extends Callable[T] with Serializable with HazelcastInstanceAware {
  @BeanProperty @transient
  var hazelcastInstance: HazelcastInstance = _
  @BeanProperty @transient
  var callerAddress:Option[Address] = _
  def call(): T = thunk(hazelcastInstance, callerAddress)
}
