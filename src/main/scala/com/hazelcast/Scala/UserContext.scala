package com.hazelcast.Scala

import scala.reflect.ClassTag
import java.util.concurrent.ConcurrentMap

object UserContext {
  class Key[T](nameOrNull: String) extends Serializable {
    def this() = this(null)
    val name = if (nameOrNull != null) nameOrNull else getClass.getName
  }
}
final class UserContext private[Scala] (private val ctx: ConcurrentMap[String, Object]) extends AnyVal {
  def get[T](key: UserContext.Key[T]): Option[T] = Option(ctx.get(key.name).asInstanceOf[T])
  def apply[T](key: UserContext.Key[T]): T = ctx.get(key.name).asInstanceOf[T] match {
    case null => sys.error(s"""Key "${key.name}" not found!""")
    case value => value
  }
  def update[T](key: UserContext.Key[T], value: T): Unit = value match {
    case null => ctx.remove(key.name)
    case _ => ctx.put(key.name, value.asInstanceOf[Object])
  }
  def putIfAbsent[T](key: UserContext.Key[T], value: T): Option[T] = value match {
    case null => get(key)
    case _ => Option(ctx.putIfAbsent(key.name, value.asInstanceOf[Object]).asInstanceOf[T])
  }
}
