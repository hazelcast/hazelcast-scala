package com.hazelcast.Scala.serialization.lz4

import com.hazelcast.Scala.serialization.SerializerEnum
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput
import net.jpountz.lz4.LZ4FastDecompressor
import net.jpountz.lz4.LZ4Compressor
import reflect.ClassTag

class CompressedSerializers(extendFrom: SerializerEnum = null)
    extends SerializerEnum(extendFrom) {

  type Compression = (LZ4Compressor, LZ4FastDecompressor)

  def High: Compression = Compression.high
  def Fast: Compression = Compression.fast

  sealed trait AbstractStreamCompressor[T] extends StreamSerializer[T] {
    def write(out: ObjectDataOutput, obj: T) = compress(out, obj)
    def read(inp: ObjectDataInput): T = inflate(inp)
    def compress(out: ObjectDataOutput, obj: T): Unit
    def inflate(inp: ObjectDataInput): T
  }
  sealed trait AbstractByteArrayCompressor[T] extends ByteArraySerializer[T] {
    def write(obj: T): Array[Byte] = compress(obj)
    def read(arr: Array[Byte]): T = inflate(arr)
    def compress(obj: T): Array[Byte]
    def inflate(arr: Array[Byte]): T
  }
  abstract class StreamCompressor[T: ClassTag](val comp: Compression)
    extends AbstractStreamCompressor[T] with StreamCompression[T]
  abstract class ByteArrayCompressor[T: ClassTag](val comp: Compression)
    extends AbstractByteArrayCompressor[T] with ByteArrayCompression[T]
}
