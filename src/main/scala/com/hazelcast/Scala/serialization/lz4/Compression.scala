package com.hazelcast.Scala.serialization.lz4

import com.hazelcast.Scala.serialization._
import com.hazelcast.nio.Bits

import net.jpountz.lz4.{ LZ4Compressor, LZ4Factory, LZ4FastDecompressor }

private[lz4] object Compression {
  def compress[R](compressor: LZ4Compressor)(bloated: Array[Byte], bloatedLen: Int)(result: (Array[Byte], Int) => R): R = {
    val maxLen = compressor.maxCompressedLength(bloatedLen)
    borrowArray(maxLen + 4) { compressTo =>
      Bits.writeIntB(compressTo, 0, bloatedLen)
      val compressedLen = compressor.compress(bloated, 0, bloatedLen, compressTo, 4)
      if (compressedLen < bloatedLen) {
        result(compressTo, compressedLen + 4)
      } else { // Compression bigger than uncompressed, use uncompressed:
        Bits.writeIntB(compressTo, 0, -bloatedLen)
        System.arraycopy(bloated, 0, compressTo, 4, bloatedLen)
        result(compressTo, bloatedLen + 4)
      }
    }
  }
  def decompress[R](decompressor: LZ4FastDecompressor)(compressed: Array[Byte])(result: (Array[Byte], Int, Int) => R): R = {
    val decompressedLen = Bits.readIntB(compressed, 0)
    if (decompressedLen > 0) {
      borrowArray(decompressedLen) { decompressTo =>
        decompressor.decompress(compressed, 4, decompressTo, 0, decompressedLen)
        result(decompressTo, 0, decompressedLen)
      }
    } else {
      result( /* not */ compressed, 4, -decompressedLen)
    }
  }
  private[this] val factory = LZ4Factory.fastestInstance
  val fast: (LZ4Compressor, LZ4FastDecompressor) = factory.fastCompressor -> factory.fastDecompressor
  val high: (LZ4Compressor, LZ4FastDecompressor) = factory.highCompressor -> factory.fastDecompressor

}
