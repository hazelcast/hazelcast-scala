package joe.schmoe

import java.util.UUID

import scala.reflect.{ ClassTag, classTag }

import com.hazelcast.Scala.serialization.SerializerEnum
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }

object TestSerializers extends SerializerEnum {

  val Employee = new StreamSerializer[Employee] {
    def write(out: ObjectDataOutput, emp: Employee): Unit = {
      out.writeObject(emp.id)
      out.writeUTF(emp.name)
      out.writeInt(emp.salary)
    }
    def read(inp: ObjectDataInput): Employee = {
      new Employee(inp.readObject.asInstanceOf[UUID], inp.readUTF, inp.readInt)
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
