package com.hazelcast.Scala.serialization

import com.hazelcast.nio.serialization.ByteArraySerializer
import com.hazelcast.map.MapInterceptor
import scala.reflect.{ ClassTag, classTag }

private[Scala] final class ByteArrayInterceptor[T <: AnyRef: ByteArraySerializer: ClassTag] extends MapInterceptor {
  private def ser = implicitly[ByteArraySerializer[T]]
  private def tag = classTag[T]

  def interceptGet(value: Object): Object = value match {
    case arr: Array[Byte] => ser.read(arr)
    case _ => value
  }
  def interceptPut(oldVal: Object, newVal: Object): Object = newVal match {
    case t: T => ser.write(t)
    case _ => newVal
  }
  def interceptRemove(value: Object): Object = value match {
    case arr: Array[Byte] => ser.read(arr)
    case _ => value
  }

  def afterGet(v: Object): Unit = ()
  def afterPut(v: Object): Unit = ()
  def afterRemove(v: Object): Unit = ()

  override def hashCode = ser.getTypeId * 37 + classTag[T].hashCode
  override def equals(obj: Any): Boolean = obj match {
    case that: ByteArrayInterceptor[T] =>
      this.ser.getTypeId == that.ser.getTypeId &&
        this.tag == that.tag
    case _ => false
  }

}
