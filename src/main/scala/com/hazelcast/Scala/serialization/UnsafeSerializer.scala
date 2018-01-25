package com.hazelcast.Scala.serialization

import java.lang.reflect.{ Field, Modifier }

import scala.util.{ Failure, Success, Try }

import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput, UnsafeHelper }

import sun.misc.Unsafe

private[serialization] object UnsafeSerializer {

  private[this] val UNSAFE = Try(Unsafe.getUnsafe) match {
    case Failure(_: SecurityException) | Success(null) => UnsafeHelper.UNSAFE.ensuring(_ != null, "Unable to obtain sun.misc.Unsafe")
    case Failure(e) => throw e
    case Success(unsafe) => unsafe
  }

  private def shouldSerialize(f: Field): Boolean =
    !Modifier.isStatic(f.getModifiers) &&
      !Modifier.isTransient(f.getModifiers)

  private def collectFields(cls: Class[_]): List[Field] = {
    if (cls != null) {
      val superFields = collectFields(cls.getSuperclass)
      val thisFields = cls.getDeclaredFields.filter(shouldSerialize).sortBy(_.getName)
      thisFields.foreach(_.setAccessible(true))
      thisFields ++: superFields
    } else Nil
  }

  private[this] val fields = new ClassValue[List[Field]] {
    def computeValue(cls: Class[_]): List[Field] = collectFields(cls)
  }

  def write(out: ObjectDataOutput, any: Any): Unit = {
    fields.get(any.getClass).foreach { field =>
      out.writeObject(field.get(any))
    }
  }
  private def collectInterfaceNames(cls: Class[_]): List[String] = {
    if (cls != null) {
      cls.getInterfaces.map(_.getName) ++: collectInterfaceNames(cls.getSuperclass)
    } else Nil
  }
  def read(inp: ObjectDataInput, cls: Class[_]): Any = {
    val instance = UNSAFE.allocateInstance(cls)
    fields.get(cls).foreach { field =>
      field.set(instance, inp.readObject[Any])
    }
    instance
  }
}
