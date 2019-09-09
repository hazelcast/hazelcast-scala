package com.hazelcast.Scala.serialization

import java.io.OutputStream
import java.util.Arrays

private[serialization] object ByteArrayOutputStream {
  private final val MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
  private def hugeCapacity(minCapacity: Int): Int = {
    if (minCapacity < 0) // overflow
      throw new OutOfMemoryError();
    if (minCapacity > MAX_ARRAY_SIZE) Integer.MAX_VALUE
    else MAX_ARRAY_SIZE
  }
  private[this] val tlOut = new SoftThreadLocal(new ByteArrayOutputStream)
  def borrow[R](thunk: ByteArrayOutputStream => R): R = {
    tlOut.use { out =>
      out.reset()
      out -> thunk(out)
    }
  }
}
private[serialization] class ByteArrayOutputStream
    extends OutputStream {

  import ByteArrayOutputStream._

  private[this] var buf = new Array[Byte](256)
  private[this] var count: Int = 0

  def withArray[T](thunk: (Array[Byte], Int) => T): T = thunk(buf, count)
  def copyArray: Array[Byte] = Arrays.copyOf(buf, count)
  private def ensureCapacity(minCapacity: Int): Unit = {
    if (minCapacity - buf.length > 0) grow(minCapacity)
  }

  private def grow(minCapacity: Int): Unit = {
    val oldCapacity = buf.length
    var newCapacity = oldCapacity << 1
    if (newCapacity - minCapacity < 0)
      newCapacity = minCapacity
    if (newCapacity - MAX_ARRAY_SIZE > 0)
      newCapacity = hugeCapacity(minCapacity)
    buf = Arrays.copyOf(buf, newCapacity)
  }
  def write(b: Int): Unit = {
    ensureCapacity(count + 1)
    buf(count) = b.asInstanceOf[Byte]
    count += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    ensureCapacity(count + len)
    System.arraycopy(b, off, buf, count, len)
    count += len
  }

  def reset(): Unit = {
    count = 0
  }

}
