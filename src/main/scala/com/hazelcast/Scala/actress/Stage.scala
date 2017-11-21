package com.hazelcast.Scala.actress

import scala.concurrent.Future

import com.hazelcast.Scala._
import com.hazelcast.core.{ HazelcastInstance, IMap }
import java.util.Map.Entry
import com.hazelcast.core.HazelcastInstanceAware
import scala.beans.BeanProperty
import com.hazelcast.nio.serialization.StreamSerializer
import com.hazelcast.nio.serialization.ByteArraySerializer
import com.hazelcast.instance.HazelcastInstanceImpl
import com.hazelcast.instance.HazelcastInstanceProxy
import java.util.Arrays

class Stage(private val actressMap: IMap[String, Array[Byte]]) {

  def this(name: String, hz: HazelcastInstance) = this(hz.getMap(name))

  def actressOf[A <: AnyRef](name: String, create: => A): ActressRef[A] =
    new ActressImpl(name, actressMap, create)

}

private class ActressImpl[A <: AnyRef](
  val name: String,
  imap: IMap[String, Array[Byte]],
  create: => A)
    extends ActressRef[A] {

  def apply[R](thunk: (HazelcastInstance, A) => R): Future[R] = {
    val ep = new SingleEntryCallbackUpdater[String, Array[Byte], R] with HazelcastInstanceAware {
      val newActress = create _
      @BeanProperty @transient
      var hazelcastInstance: HazelcastInstance = _
      var newState: Array[Byte] = _
      def onEntry(entry: Entry[String, Array[Byte]]): R = {
        val serializationService = hazelcastInstance match {
          case hz: HazelcastInstanceImpl => hz.getSerializationService
          case hz: HazelcastInstanceProxy => hz.getSerializationService
        }
        val actress = entry.value match {
          case null => newActress()
          case bytes =>
            val inp = serializationService.createObjectDataInput(bytes)
            serializationService.readObject(inp)
        }
        val result = thunk(hazelcastInstance, actress)
        newState = {
          val out = serializationService.createObjectDataOutput()
          serializationService.writeObject(out, actress)
          out.toByteArray()
        }
        entry.value = newState
        result
      }
      override def processBackup(entry: Entry[String, Array[Byte]]): Unit = {
        entry.value = newState
      }
    }
    val callback = ep.newCallback()
    imap.submitToKey(name, ep, callback)
    callback.future
  }
}
