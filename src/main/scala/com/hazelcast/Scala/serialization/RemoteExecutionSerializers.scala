package com.hazelcast.Scala.serialization

import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput
import scala.collection.concurrent.TrieMap
import scala.util.Try
import scala.reflect.ClassTag
import com.hazelcast.map.EntryProcessor
import java.util.concurrent.Callable
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.OutputStream
import java.io.InputStream

/**
  * Serializers for remote execution.
  * NOTE: Do not use for production use.
  * Not only is the code experimental, it's
  * very inefficient.
  */
object RemoteExecutionSerializers extends RemoteExecutionSerializers {
  protected def serializeBytecodeFor(cls: Class[_]) = true
}

abstract class RemoteExecutionSerializers extends SerializerEnum(DefaultSerializers) {
  protected def serializeBytecodeFor(cls: Class[_]): Boolean
  private[this] val classBytes = new ClassValue[Option[ByteArrayClassLoader]] {
    private[this] val excludePackages = Set("com.hazelcast.", "scala.")
    private def include(cls: Class[_]): Boolean = !excludePackages.exists(cls.getName.startsWith) && serializeBytecodeFor(cls)
    def computeValue(cls: Class[_]): Option[ByteArrayClassLoader] =
      if (include(cls)) {
        Try(ByteArrayClassLoader(cls)).toOption
      } else None
  }

  private class ClassBytesSerializer[T: ClassTag] extends StreamSerializer[T] {
    def write(out: ObjectDataOutput, any: T): Unit = {
      out.writeUTF(any.getClass.getName)
      classBytes.get(any.getClass) match {
        case None => out.writeByteArray(Array.emptyByteArray)
        case Some(cl) => out.writeByteArray(cl.bytes)
      }
      UnsafeSerializer.write(out, any)
    }
    def read(inp: ObjectDataInput): T = {
      val className = inp.readUTF()
      val classBytes = inp.readByteArray()
      val cls =
        if (classBytes.length == 0) {
          Class.forName(className)
        } else {
          val cl = new ByteArrayClassLoader(className, classBytes)
          Class.forName(className, true, cl)
        }
      UnsafeSerializer.read(inp, cls).asInstanceOf[T]
    }
  }

  type S[T] = StreamSerializer[T]

  val Function0Ser: S[Function0[_]] = new ClassBytesSerializer
  val Function1Ser: S[Function1[_, _]] = new ClassBytesSerializer
  val Function2Ser: S[Function2[_, _, _]] = new ClassBytesSerializer
  val PartialFunctionSer: S[PartialFunction[_, _]] = new ClassBytesSerializer
  val EntryProcessorSer: S[EntryProcessor[_, _]] = new ClassBytesSerializer
  val CallableSer: S[Callable[_]] = new ClassBytesSerializer
  val RunnableSer: S[Runnable] = new ClassBytesSerializer

}
