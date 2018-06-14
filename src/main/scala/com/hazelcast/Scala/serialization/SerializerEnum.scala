package com.hazelcast.Scala.serialization

import com.hazelcast.config.SerializationConfig
import scala.reflect.{ ClassTag, classTag }
import com.hazelcast.nio.serialization.Serializer
import com.hazelcast.config.SerializerConfig
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput

abstract class SerializerEnum private (offsetOrExtends: Either[Int, Option[SerializerEnum]]) extends Enumeration {
  def this(offset: Int) = this(Left(offset))
  def this(extendFrom: SerializerEnum) = this(Right(Option(extendFrom)))
  def this() = this(Right(None))

  type Value = ClassSerializer[_]

  private val (offset: Int, extendFrom) = offsetOrExtends match {
    case Left(offset) =>
      offset -> None
    case Right(None) =>
      0 -> None
    case Right(Some(extendFrom)) =>
      math.max(0, extendFrom.maxId + 1 + extendFrom.offset) -> Some(extendFrom)
  }

  sealed abstract class ClassSerializer[T: ClassTag] extends Val with Serializer {
    val theClass = classTag[T].runtimeClass.asInstanceOf[Class[T]]
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
  class SingletonSerializer[T: ClassTag](singleton: T) extends ClassSerializer[T] with com.hazelcast.nio.serialization.StreamSerializer[T] {
    def write(out: ObjectDataOutput, obj: T) = assert(obj == singleton)
    def read(inp: ObjectDataInput): T = singleton
  }

  def register(conf: SerializationConfig): Unit = {
    extendFrom.foreach(_.register(conf))
    serializers.foreach(_.register(conf))
  }

  def serializers: Iterator[ClassSerializer[_]] = this.values.iterator.map(_.asInstanceOf[ClassSerializer[_]])
}
