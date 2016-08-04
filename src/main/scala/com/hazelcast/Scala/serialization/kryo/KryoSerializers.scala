package com.hazelcast.Scala.serialization.kryo

import com.hazelcast.Scala.serialization.SerializerEnum
import com.esotericsoftware.kryo.Kryo
import org.objenesis.strategy.StdInstantiatorStrategy
import com.esotericsoftware.kryo.pool.KryoFactory
import scala.reflect.ClassTag
import com.esotericsoftware.kryo.io.Output
import KryoSerializers._
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput
import com.esotericsoftware.kryo.io.Input
import java.lang.ref.SoftReference
import java.util.Arrays
import com.hazelcast.Scala.serialization.SoftThreadLocal

private object KryoSerializers {
  private val defaultStrategy = new StdInstantiatorStrategy
  def defaultIO = new Input(512) -> new Output(512, Int.MaxValue)
  def defaultKryo = {
    val kryo = new Kryo
    kryo.setInstantiatorStrategy(defaultStrategy)
    kryo
  }
  final class KryoThreadLocal(newKryoIO: => (Kryo, Input, Output))
      extends SoftThreadLocal(newKryoIO) {

    def read[T](thunk: (Kryo, Input) => T): T = use {
      case v @ (kryo, inp, _) =>
        inp.rewind()
        v -> thunk(kryo, inp)
    }

    def write[T](thunk: (Kryo, Output) => T): T = use {
      case v @ (kryo, _, out) =>
        out.clear()
        v -> thunk(kryo, out)
    }
  }
}

class KryoSerializers(
  extendFrom: SerializerEnum = null,
  newIO: => (Input, Output) = defaultIO,
  newKryo: => Kryo = defaultKryo)
    extends SerializerEnum(extendFrom) {

  private lazy val factory = new KryoFactory {
    def create: Kryo = {
      val kryo = newKryo
      serializers.foreach { ser =>
        kryo.register(ser.theClass, ser.getTypeId)
      }
      kryo
    }
  }

  private[this] val threadLocal = new KryoThreadLocal({
    val (inp, out) = newIO
    (factory.create, inp, out)
  })

  class KryoStreamSerializer[T: ClassTag] extends StreamSerializer[T] {
    def write(out: ObjectDataOutput, obj: T) {
      threadLocal.write {
        case (kryo, kOut) =>
          kryo.writeObject(kOut, obj)
          out.writeInt(kOut.position)
          out.write(kOut.getBuffer, 0, kOut.position)
      }
    }
    def read(inp: ObjectDataInput): T = {
      threadLocal.read {
        case (kryo, kInp) =>
          val size = inp.readInt
          if (size > kInp.getBuffer.length) {
            kInp.setBuffer(new Array[Byte](size))
          }
          kInp.setLimit(size)
          inp.readFully(kInp.getBuffer, 0, size)
          kryo.readObject(kInp, theClass)
      }
    }
  }

  class KryoByteArraySerializer[T: ClassTag] extends ByteArraySerializer[T] {
    def write(obj: T): Array[Byte] = {
      threadLocal.write {
        case (kryo, kOut) =>
          kryo.writeObject(kOut, obj)
          Arrays.copyOf(kOut.getBuffer, kOut.position)
      }
    }
    def read(arr: Array[Byte]): T = {
      threadLocal.read {
        case (kryo, kInp) =>
          val temp = kInp.getBuffer
          try {
            kInp.setBuffer(arr)
            kryo.readObject(kInp, theClass)
          } finally {
            kInp.setBuffer(temp)
          }
      }
    }
  }

}
