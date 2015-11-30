package com.hazelcast.Scala

import scala.reflect.ClassTag
import java.util.concurrent.ConcurrentMap

object UserContext {
  class Key[T](nameOrNull: String) {
    def this() = this(null)
    val name = if (nameOrNull != null) nameOrNull else getClass.getName
    private[UserContext] def get(ctx: ConcurrentMap[String, Object]): Option[T] = Option(ctx.get(name).asInstanceOf[T])
    private[UserContext] def apply(ctx: ConcurrentMap[String, Object]): T = ctx.get(name).asInstanceOf[T] match {
      case null => sys.error(s"""Key "$name" not found!""")
      case value => value
    }
    private[UserContext] def update(ctx: ConcurrentMap[String, Object], value: T): Unit = value match {
      case null => ctx.remove(name)
      case _ => ctx.put(name, value.asInstanceOf[Object])
    }
  }
}
final class UserContext private[Scala] (private val ctx: ConcurrentMap[String, Object]) extends AnyVal {
  def get[T](key: UserContext.Key[T]): Option[T] = key.get(ctx)
  def apply[T](key: UserContext.Key[T]): T = key(ctx)
  def update[T](key: UserContext.Key[T], value: T): Unit = key(ctx) = value
}
