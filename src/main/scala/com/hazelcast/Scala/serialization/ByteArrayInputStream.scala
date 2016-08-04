package com.hazelcast.Scala.serialization

import java.io.InputStream

private[serialization] class ByteArrayInputStream(buf: Array[Byte], offset: Int, length: Int)
    extends InputStream {

  private[this] var pos = offset
  private[this] var count = (offset + length) min buf.length
  private[this] var mark = offset

  def read(): Int = if (pos >= count) -1 else {
    pos += 1
    buf(pos - 1) & 0xff
  }

  override def read(b: Array[Byte], off: Int, length: Int): Int = {
    if (pos >= count) -1
    else {
      var len = length
      val avail = count - pos
      if (len > avail) {
        len = avail
      }
      if (len <= 0) 0
      else {
        System.arraycopy(buf, pos, b, off, len)
        pos += len
        len
      }
    }
  }

  override def skip(n: Long): Long = {
    var k = count - pos
    if (n < k) {
      k = if (n < 0) 0 else n.asInstanceOf[Int]
    }
    pos += k
    k
  }

  override def available(): Int = count - pos
  override def markSupported() = true
  override def mark(readAheadLimit: Int): Unit = {
    mark = pos
  }

  override def reset() {
    pos = mark
  }

}
