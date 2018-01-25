package com.hazelcast.Scala

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
  def getOrElseUpdate[T](key: UserContext.Key[T], create: => T): T = {
      def getOrUpdate(locked: Boolean): T = {
        get(key) match {
          case Some(value) => value
          case None if locked =>
            val value: T = create
            putIfAbsent(key, value) getOrElse value
          case None =>
            key.name.intern.synchronized(getOrUpdate(locked = true))
        }
      }
    getOrUpdate(locked = false)
  }
}
