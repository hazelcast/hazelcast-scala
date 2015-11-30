package com.hazelcast.Scala

private[Scala] object Types {
  val PrimitiveWrappers: Map[Class[_], Class[_]] = Map(
    classOf[Boolean] -> classOf[java.lang.Boolean],
    classOf[Byte] -> classOf[java.lang.Byte],
    classOf[Short] -> classOf[java.lang.Short],
    classOf[Char] -> classOf[java.lang.Character],
    classOf[Float] -> classOf[java.lang.Float],
    classOf[Int] -> classOf[java.lang.Integer],
    classOf[Double] -> classOf[java.lang.Double],
    classOf[Long] -> classOf[java.lang.Long]
  )
}
