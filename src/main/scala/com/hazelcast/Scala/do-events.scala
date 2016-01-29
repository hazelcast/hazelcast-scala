package com.hazelcast.Scala

import com.hazelcast.core.DistributedObject

sealed trait DistributedObjectChange

case class DistributedObjectCreated(name: String, obj: DistributedObject) extends DistributedObjectChange
case class DistributedObjectDestroyed(name: String, service: String) extends DistributedObjectChange
