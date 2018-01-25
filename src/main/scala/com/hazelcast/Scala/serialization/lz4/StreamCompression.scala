package com.hazelcast.Scala.serialization.lz4

import net.jpountz.lz4._
import com.hazelcast.nio.serialization.StreamSerializer
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import com.hazelcast.Scala.serialization._

trait StreamCompression[T] extends StreamSerializer[T] {

  protected def comp: (LZ4Compressor, LZ4FastDecompressor)

  abstract override def write(out: ObjectDataOutput, t: T): Unit = {
    ByteArrayOutputStream.borrow { baOut =>
      super.write(ObjectDataOutputStreamProxy(baOut, out), t)
      baOut.withArray {
        case (array, len) =>
          Compression.compress(comp._1)(array, len) {
            case (compressed, len) =>
              out.writeBytes(compressed, len)
          }
      }
    }
  }
  abstract override def read(inp: ObjectDataInput): T =
    inp.readBytes {
      case (compressed, _) =>
        Compression.decompress(comp._2)(compressed) {
          case (decompressed, decompOffset, decompLen) =>
            val baInp = new ByteArrayInputStream(decompressed, decompOffset, decompLen)
            val inpProxy = ObjectDataInputStreamProxy(baInp, inp)
            super.read(inpProxy)
        }
    }
}
trait HighStreamCompression[T] extends StreamCompression[T] {
  protected def comp: (LZ4Compressor, LZ4FastDecompressor) = Compression.high
}
trait FastStreamCompression[T] extends StreamCompression[T] {
  protected def comp: (LZ4Compressor, LZ4FastDecompressor) = Compression.fast
}
