package com.hazelcast.Scala.serialization

import java.util.Arrays
import java.util.concurrent.Callable
import scala.reflect.ClassTag
import com.hazelcast.Scala.Aggregator
import com.hazelcast.Scala.Pipe
import com.hazelcast.map.EntryBackupProcessor
import com.hazelcast.map.EntryProcessor
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.core.IFunction
import com.hazelcast.query.Predicate
import java.util.Comparator

/**
  * Serializers for dynamic execution.
  * NOTE: Not intended for production use.
  * Not only is the code experimental, it's
  * very inefficient.
  */
object DynamicExecution extends DynamicExecution {
  protected def serializeBytecodeFor(cls: Class[_]) = true
}

abstract class DynamicExecution extends SerializerEnum(Defaults) {
  protected def serializeBytecodeFor(cls: Class[_]): Boolean
  private[this] val loaderByClass = new ClassValue[Option[ByteArrayClassLoader]] {
    private[this] val excludePackages = Set("com.hazelcast.", "scala.", "java.", "javax.")
    private def include(cls: Class[_]): Boolean = !excludePackages.exists(cls.getName.startsWith) && serializeBytecodeFor(cls)
    def computeValue(cls: Class[_]): Option[ByteArrayClassLoader] =
      if (include(cls)) {
        try {
          Some(ByteArrayClassLoader(cls))
        } catch {
          case ncdf: NoClassDefFoundError =>
            classByName.get(cls.getName) match {
              case Some((bytes, classForBytes)) if cls == classForBytes => Some(new ByteArrayClassLoader(cls.getName, bytes))
              case _ => throw ncdf
            }
        }
      } else None
  }
  private[this] val classByName = new collection.concurrent.TrieMap[String, (Array[Byte], Class[_])]

  private class ClassBytesSerializer[T: ClassTag] extends StreamSerializer[T] {
    def write(out: ObjectDataOutput, any: T): Unit = {
      out.writeUTF(any.getClass.getName)
      loaderByClass.get(any.getClass) match {
        case Some(cl) => out.writeByteArray(cl.bytes)
        case _ => out.writeByteArray(Array.emptyByteArray)
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
          classByName.get(className) match {
            case Some((bytes, cls)) if Arrays.equals(classBytes, bytes) => cls
            case _ =>
              val cl = new ByteArrayClassLoader(className, classBytes)
              val cls = Class.forName(className, true, cl)
              classByName.put(className, classBytes -> cls)
              cls
          }
        }
      UnsafeSerializer.read(inp, cls).asInstanceOf[T]
    }
  }

  type S[T] = StreamSerializer[T]

  val Function0Ser: S[Function0[_]] = new ClassBytesSerializer
  val Function1Ser: S[Function1[_, _]] = new ClassBytesSerializer
  val Function2Ser: S[Function2[_, _, _]] = new ClassBytesSerializer
  val Function3Ser: S[Function3[_, _, _, _]] = new ClassBytesSerializer
  val PartialFunctionSer: S[PartialFunction[_, _]] = new ClassBytesSerializer
  val EntryProcessorSer: S[EntryProcessor[_, _]] = new ClassBytesSerializer
  val EntryBackupProcessorSer: S[EntryBackupProcessor[_, _]] = new ClassBytesSerializer
  val CallableSer: S[Callable[_]] = new ClassBytesSerializer
  val RunnableSer: S[Runnable] = new ClassBytesSerializer
  val PredicateSer: S[Predicate[_, _]] = new ClassBytesSerializer
  val PipeSer: S[Pipe[_]] = new ClassBytesSerializer
  val AggregatorSer: S[Aggregator[_, _]] = new ClassBytesSerializer
  val ComparatorSer: S[Comparator[_]] = new ClassBytesSerializer
  val IFunctionSer: S[IFunction[_, _]] = new ClassBytesSerializer

}
