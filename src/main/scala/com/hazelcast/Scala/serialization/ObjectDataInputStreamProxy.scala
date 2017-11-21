package com.hazelcast.Scala.serialization

import java.io.OutputStream
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream
import com.hazelcast.internal.serialization.{InternalSerializationService => SerializationService}
import java.util.Arrays
import java.io.InputStream
import java.lang.reflect.Field
import com.hazelcast.nio.serialization.HazelcastSerializationException
import com.hazelcast.nio.ObjectDataInput

private[serialization] object ObjectDataInputStreamProxy {
  private[this] val SerSvcClass = classOf[SerializationService]
  private[this] val SerSvcField = new ClassValue[Field] {
    def computeValue(cls: Class[_]): Field = {
      val field = findField(cls)
      field setAccessible true
      field
    }
    private def findField(cls: Class[_]): Field = {
      if (cls == null) throw new HazelcastSerializationException(s"Cannot find field for $SerSvcClass")
      else {
        val maybeField = cls.getDeclaredFields.find(fld => SerSvcClass.isAssignableFrom(fld.getType))
        maybeField getOrElse findField(cls.getSuperclass)
      }
    }
  }
  private[this] def getSerializationService(out: ObjectDataInput): SerializationService = {
    val field = SerSvcField.get(out.getClass)
    field.get(out).asInstanceOf[SerializationService]
  }
  def apply(is: ByteArrayInputStream, inp: ObjectDataInput) = new ObjectDataInputStreamProxy(is, getSerializationService(inp))
}

private[serialization] class ObjectDataInputStreamProxy(
  is: ByteArrayInputStream,
  ss: SerializationService)
    extends ObjectDataInputStream(is, ss)
