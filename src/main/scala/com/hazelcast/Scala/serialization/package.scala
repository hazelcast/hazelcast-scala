package com.hazelcast.Scala

import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput

package object serialization {
  private[this] val tlArray = new SoftThreadLocal(new Array[Byte](256))
  def borrowArray[R](minSize: Int)(thunk: Array[Byte] => R): R = {
      def computeSize(minSize: Int, arrSize: Int): Int = {
        val newSize = arrSize << 1
        if (newSize >= minSize) newSize
        else computeSize(minSize, newSize)
      }
    tlArray.use { array =>
      val newArray = minSize > array.length
      val buf = if (newArray) new Array[Byte](computeSize(minSize, array.length)) else array
      buf -> thunk(buf)
    }
  }

  implicit class DataOut(private val out: ObjectDataOutput) extends AnyVal {
    def writeBytes(arr: Array[Byte], len: Int): Unit = {
      out.writeInt(len)
      out.write(arr, 0, len)
    }
  }
  implicit class DataInp(private val inp: ObjectDataInput) extends AnyVal {
    def readBytes[R](result: (Array[Byte], Int) => R): R = {
      val len = inp.readInt()
      borrowArray(len) { arr =>
        inp.readFully(arr, 0, len)
        result(arr, len)
      }
    }
  }

}
