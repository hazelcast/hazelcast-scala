package com.hazelcast.Scala.serialization

import com.hazelcast.config.SerializationConfig
import scala.reflect.{ ClassTag, classTag }
import com.hazelcast.nio.serialization.Serializer
import com.hazelcast.config.SerializerConfig

abstract class SerializerEnum(private val offset: Int) extends Enumeration {
  type Value = ClassSerializer[_]
  def this(extendFrom: SerializerEnum) = this(extendFrom.maxId + 1 + extendFrom.offset)
  def this() = this(0)

  sealed abstract class ClassSerializer[T: ClassTag] extends Val with Serializer {
    def theClass = classTag[T].runtimeClass
    def register(conf: SerializationConfig): Unit = {
      val serConf = new SerializerConfig
      serConf.setImplementation(this).setTypeClass(theClass)
      conf.addSerializerConfig(serConf)
    }
    final def getTypeId = this.id + 1 + offset
    def destroy = ()
  }

  abstract class ByteArraySerializer[T: ClassTag] extends ClassSerializer[T] with com.hazelcast.nio.serialization.ByteArraySerializer[T]
  abstract class StreamSerializer[T: ClassTag] extends ClassSerializer[T] with com.hazelcast.nio.serialization.StreamSerializer[T]

  def register(conf: SerializationConfig): Unit = serializers.foreach(_.register(conf))

  def serializers: Iterator[ClassSerializer[_]] = this.values.iterator.map(_.asInstanceOf[ClassSerializer[_]])
}
