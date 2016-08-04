package joe.schmoe

import java.util.UUID

import com.hazelcast.Scala.serialization.kryo.KryoSerializers
import com.hazelcast.Scala.serialization.lz4.CompressedSerializers
import com.hazelcast.Scala.serialization.lz4.FastStreamCompression
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput

object TestSerializers extends CompressedSerializers {

  val Employee = new StreamCompressor[Employee](High) {
    def compress(out: ObjectDataOutput, emp: Employee): Unit = {
      out.writeObject(emp.id)
      out.writeUTF(emp.name)
      out.writeInt(emp.salary)
      out.writeInt(emp.age)
      out.writeBoolean(emp.active)
    }
    def inflate(inp: ObjectDataInput): Employee = {
      new Employee(inp.readObject.asInstanceOf[UUID], inp.readUTF, inp.readInt, inp.readInt, inp.readBoolean)
    }
  }

  val Weather = new StreamSerializer[Weather] {
    def write(out: ObjectDataOutput, w: Weather): Unit = {
      out.writeFloat(w.tempMin)
      out.writeFloat(w.tempMax)
    }
    def read(inp: ObjectDataInput): Weather = {
      new Weather(inp.readFloat, inp.readFloat)
    }
  }

}

object TestKryoSerializers extends KryoSerializers(TestSerializers) {
  val StatsSer = new KryoStreamSerializer[Stats] with FastStreamCompression[Stats]
}
