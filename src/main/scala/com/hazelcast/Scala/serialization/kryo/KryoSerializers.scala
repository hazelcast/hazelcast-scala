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

private object KryoSerializers {
  private val defaultStrategy = new StdInstantiatorStrategy
  def defaultIO = new Input(4096) -> new Output(4096, Int.MaxValue)
  def defaultKryo = {
    val kryo = new Kryo
    kryo.setInstantiatorStrategy(defaultStrategy)
    kryo
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

  private[this] object threadLocal extends ThreadLocal[SoftReference[(Kryo, Input, Output)]] {
    def newRef = {
      val (inp, out) = newIO
      new SoftReference((factory.create, inp, out))
    }
    override def initialValue = newRef

    def read[T](thunk: (Kryo, Input) => T): T = {
      this.get().get() match {
        case null =>
          this.set(newRef)
          read(thunk)
        case (kryo, inp, _) =>
          inp.rewind()
          thunk(kryo, inp)
      }
    }

    def write[T](thunk: (Kryo, Output) => T): T = {
      this.get().get() match {
        case null =>
          this.set(newRef)
          write(thunk)
        case (kryo, _, out) =>
          out.clear()
          thunk(kryo, out)
      }
    }
  }

  class KryoSerializer[T: ClassTag] extends StreamSerializer[T] {
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

}
