package com.hazelcast.Scala.serialization

import com.hazelcast.nio.serialization.StreamSerializer
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput

trait RemoteSerializer[T] extends StreamSerializer[T] {
  def destroy = ()
  def write(out: ObjectDataOutput, ref: T): Unit = {
    val cl = ByteArrayClassLoader(ref.getClass)
    out.writeUTF(cl.className)
    out.writeByteArray(cl.bytes)
    out.writeObject(ref)
  }
  def read(inp: ObjectDataInput): T = {
    val ccl = Thread.currentThread.getContextClassLoader
    Thread.currentThread setContextClassLoader new ByteArrayClassLoader(inp.readUTF, inp.readByteArray, ccl)
    try {
      inp.readObject()
    } finally {
      Thread.currentThread setContextClassLoader ccl
    }
  }
}
