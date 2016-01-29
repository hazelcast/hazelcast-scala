package com.hazelcast.Scala.serialization

import java.lang.reflect.Field
import java.lang.reflect.Modifier

import scala.util._

import com.hazelcast.nio.ObjectDataInput

import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.UnsafeHelper

private[serialization] object UnsafeSerializer {

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
  def read(inp: ObjectDataInput, cls: Class[_]): Any = {
    val instance = UnsafeHelper.UNSAFE.allocateInstance(cls)
    fields.get(cls).foreach(_.set(instance, inp.readObject))
    instance
  }
}
