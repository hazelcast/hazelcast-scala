package com.hazelcast.Scala.dds

private[Scala] case class Sorted[O](ordering: Ordering[O], skip: Int, limit: Option[Int])
