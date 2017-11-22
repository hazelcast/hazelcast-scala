package com.hazelcast.Scala.serialization.lz4

import java.util.Arrays

import com.hazelcast.nio.Bits

import com.hazelcast.nio.serialization.ByteArraySerializer

import net.jpountz.lz4.LZ4Compressor
import net.jpountz.lz4.LZ4FastDecompressor

trait ByteArrayCompression[T] extends ByteArraySerializer[T] {
  protected def comp: (LZ4Compressor, LZ4FastDecompressor)

  abstract override def write(obj: T): Array[Byte] = {
    val serialized = super.write(obj)
    Compression.compress(comp._1)(serialized, serialized.length) {
      case (compressed, len) =>
        Arrays.copyOf(compressed, len)
    }
  }

  abstract override def read(compressed: Array[Byte]): T = {
    Compression.decompress(comp._2)(compressed) {
      case (decompressed, offset, len) =>
        super.read(Arrays.copyOfRange(decompressed, offset, offset + len))
    }
  }
}
trait HighByteArrayCompression[T] extends ByteArrayCompression[T] {
  protected def comp: (LZ4Compressor, LZ4FastDecompressor) = Compression.high
}
trait FastByteArrayCompression[T] extends ByteArrayCompression[T] {
  protected def comp: (LZ4Compressor, LZ4FastDecompressor) = Compression.fast
}
