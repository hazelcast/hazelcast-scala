package com.hazelcast.Scala.serialization

import java.math.{ MathContext, RoundingMode }
import java.util.{ Comparator, UUID }

import scala.annotation.tailrec
import scala.collection.mutable.Builder
import scala.reflect.ClassTag
import scala.util.{ Either, Failure, Left, Right, Success, Try }

import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }

import scala.collection.immutable.Nil

object DefaultSerializers extends SerializerEnum(-987654321) {

  val OptionSer = new OptionSerializer[Option[_]]
  val SomeSer = new OptionSerializer[Some[_]]
  val NoneSer = new OptionSerializer[None.type]

  val TrySer = new TrySerializer[Try[_]]
  val SuccessSer = new TrySerializer[Success[_]]
  val FailureSer = new TrySerializer[Failure[_]]

  val EitherSer = new EitherSerializer[Either[_, _]]
  val LeftSer = new EitherSerializer[Left[_, _]]
  val RightSer = new EitherSerializer[Right[_, _]]

  val ListSer = new ListSerializer[List[_]]
  val NilSer = new ListSerializer[Nil.type]
  val NEListSer = new ListSerializer[::[_]]

  val ClassTagSer = new StreamSerializer[ClassTag[_]] {
    def write(out: ObjectDataOutput, tag: ClassTag[_]): Unit = out.writeObject(tag.runtimeClass)
    def read(inp: ObjectDataInput): ClassTag[_] = ClassTag(inp.readObject)
  }

  val UUIDSer = new StreamSerializer[UUID] {
    def write(out: ObjectDataOutput, uuid: UUID): Unit = {
      out.writeLong(uuid.getMostSignificantBits)
      out.writeLong(uuid.getLeastSignificantBits)
    }
    def read(inp: ObjectDataInput): UUID = new UUID(inp.readLong, inp.readLong)
  }

  private type IHashMap = collection.immutable.HashMap[Any, Any]
  val IHashMapSer = new StreamSerializer[IHashMap] {
    def write(out: ObjectDataOutput, map: IHashMap): Unit = {
      out.writeInt(map.size)
      writeSMap(map, out)
    }
    def read(inp: ObjectDataInput): IHashMap = {
      val len = inp.readInt()
      readIMap(len, new IHashMap, inp)
    }
  }
  private type ITreeMap = collection.immutable.TreeMap[Any, Any]
  val ITreeMapSer = new StreamSerializer[ITreeMap] {
    def write(out: ObjectDataOutput, map: ITreeMap): Unit = {
      out.writeObject(map.ordering)
      out.writeInt(map.size)
      writeSMap(map, out)
    }
    def read(inp: ObjectDataInput): ITreeMap = {
      val ord = inp.readObject[Ordering[Any]]
      val len = inp.readInt()
      readIMap(len, new ITreeMap()(ord), inp)
    }
  }
  private type CTrieMap = collection.concurrent.TrieMap[Any, Any]
  val CTrieMapSer = new StreamSerializer[CTrieMap] {
    def write(out: ObjectDataOutput, m: CTrieMap): Unit = {
      out.writeInt(m.size)
      writeSMap(m, out)
    }
    def read(inp: ObjectDataInput): CTrieMap = {
      val len = inp.readInt()
      val builder = collection.concurrent.TrieMap.newBuilder[Any, Any]
      builder.sizeHint(len)
      readMutable(len, builder, () => Tuple2Ser.read(inp))
    }
  }
  private type MHashMap = collection.mutable.HashMap[Any, Any]
  val MHashMapSer = new StreamSerializer[MHashMap] {
    def write(out: ObjectDataOutput, m: MHashMap): Unit = {
      out.writeInt(m.size)
      writeSMap(m, out)
    }
    def read(inp: ObjectDataInput): MHashMap = {
      val len = inp.readInt()
      val builder = collection.mutable.HashMap.newBuilder[Any, Any]
      builder.sizeHint(len)
      readMutable(len, builder, () => Tuple2Ser.read(inp))
    }
  }
  private type MTreeSet = collection.mutable.TreeSet[Any]
  val MTreeSetSer = new StreamSerializer[MTreeSet] {
    def write(out: ObjectDataOutput, s: MTreeSet): Unit = {
      out.writeObject(s.ordering)
      out.writeInt(s.size)
      s.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): MTreeSet = {
      val ord = inp.readObject[Ordering[Any]]
      val len = inp.readInt()
      val builder = collection.mutable.TreeSet.newBuilder[Any](ord)
      builder.sizeHint(len)
      readMutable(len, builder, () => inp.readObject[Any])
    }
  }
  private type ArrayBuffer = collection.mutable.ArrayBuffer[Any]
  val ArrayBufferSer = new StreamSerializer[ArrayBuffer] {
    def write(out: ObjectDataOutput, buf: ArrayBuffer): Unit = {
      out.writeInt(buf.size)
      buf.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): ArrayBuffer = {
      val size = inp.readInt()
      val builder = collection.mutable.ArrayBuffer.newBuilder[Any]
      builder.sizeHint(size)
      readMutable(size, builder, () => inp.readObject[Any])
    }
  }
  private type ITreeSet = collection.immutable.TreeSet[Any]
  val ITreeSetSer = new StreamSerializer[ITreeSet] {
    def write(out: ObjectDataOutput, s: ITreeSet): Unit = {
      out.writeObject(s.ordering)
      out.writeInt(s.size)
      s.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): ITreeSet = {
      val ord = inp.readObject[Ordering[Any]]
      val len = inp.readInt()
      val builder = collection.immutable.TreeSet.newBuilder[Any](ord)
      builder.sizeHint(len)
      readMutable(len, builder, () => inp.readObject[Any])
    }
  }
  private type JHashMap = java.util.HashMap[Any, Any]
  val JHashMapSer = new StreamSerializer[JHashMap] {
    def write(out: ObjectDataOutput, m: JHashMap): Unit = {
      out.writeInt(m.size)
      writeJMap(m, out)
    }
    def read(inp: ObjectDataInput): JHashMap = {
      val size = inp.readInt()
      readJMap(size, new JHashMap((size * 1.4f).toInt, 1f), inp)
    }
  }
  private type JTreeMap = java.util.TreeMap[Any, Any]
  val JTreeMapSer = new StreamSerializer[JTreeMap] {
    def write(out: ObjectDataOutput, m: JTreeMap): Unit = {
      out.writeObject(m.comparator)
      out.writeInt(m.size)
      writeJMap(m, out)
    }
    def read(inp: ObjectDataInput): JTreeMap = {
      val comp = inp.readObject[Comparator[Any]]
      val size = inp.readInt
      readJMap(size, new JTreeMap(comp), inp)
    }
  }

  val VectorSer = new StreamSerializer[Vector[Any]] {
    def write(out: ObjectDataOutput, v: Vector[Any]): Unit = {
      out.writeInt(v.length)
      v.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): Vector[Any] = {
      val len = inp.readInt
        @tailrec
        def buildVector(v: Vector[Any] = Vector.empty, idx: Int = 0): Vector[Any] = {
          if (idx == len) v
          else buildVector(v :+ inp.readObject[Any], idx + 1)
        }
      buildVector()
    }
  }
  val Tuple2Ser = new StreamSerializer[Tuple2[_, _]] {
    def write(out: ObjectDataOutput, t: Tuple2[_, _]): Unit = {
      out.writeObject(t._1)
      out.writeObject(t._2)
    }
    def read(inp: ObjectDataInput): Tuple2[_, _] = {
      (inp.readObject, inp.readObject)
    }
  }
  val Tuple3Ser = new StreamSerializer[Tuple3[_, _, _]] {
    def write(out: ObjectDataOutput, t: Tuple3[_, _, _]): Unit = {
      out.writeObject(t._1)
      out.writeObject(t._2)
      out.writeObject(t._3)
    }
    def read(inp: ObjectDataInput): Tuple3[_, _, _] = {
      (inp.readObject, inp.readObject, inp.readObject)
    }
  }
  val Tuple4Ser = new StreamSerializer[Tuple4[_, _, _, _]] {
    def write(out: ObjectDataOutput, t: Tuple4[_, _, _, _]): Unit = {
      out.writeObject(t._1)
      out.writeObject(t._2)
      out.writeObject(t._3)
      out.writeObject(t._4)
    }
    def read(inp: ObjectDataInput): Tuple4[_, _, _, _] = {
      (inp.readObject, inp.readObject, inp.readObject, inp.readObject)
    }
  }
  val Tuple5Ser = new StreamSerializer[Tuple5[_, _, _, _, _]] {
    def write(out: ObjectDataOutput, t: Tuple5[_, _, _, _, _]): Unit = {
      out.writeObject(t._1)
      out.writeObject(t._2)
      out.writeObject(t._3)
      out.writeObject(t._4)
      out.writeObject(t._5)
    }
    def read(inp: ObjectDataInput): Tuple5[_, _, _, _, _] = {
      (inp.readObject, inp.readObject, inp.readObject, inp.readObject, inp.readObject)
    }
  }

  val MathCtxSer = new StreamSerializer[MathContext] {
    def write(out: ObjectDataOutput, mc: MathContext): Unit = {
      out.writeInt(mc.getPrecision)
      out.writeObject(mc.getRoundingMode)
    }
    def read(inp: ObjectDataInput) =
      new MathContext(inp.readInt, inp.readObject.asInstanceOf[RoundingMode])
  }
  val BigDecSer = new StreamSerializer[BigDecimal] {
    def write(out: ObjectDataOutput, bd: BigDecimal): Unit = {
      out.writeObject(bd.underlying)
      MathCtxSer.write(out, bd.mc)
    }
    def read(inp: ObjectDataInput): BigDecimal =
      new BigDecimal(inp.readObject.asInstanceOf[java.math.BigDecimal], MathCtxSer.read(inp))
  }
  val BigIntSer = new StreamSerializer[BigInt] {
    def write(out: ObjectDataOutput, bi: BigInt): Unit = out.writeObject(bi.underlying)
    def read(inp: ObjectDataInput): BigInt = new BigInt(inp.readObject.asInstanceOf[java.math.BigInteger])
  }

  final class OptionSerializer[O <: Option[_]: ClassTag] extends StreamSerializer[O] {
    def write(out: ObjectDataOutput, opt: O): Unit =
      if (opt.isEmpty) {
        out.write(0)
      } else {
        out.write(1)
        out.writeObject(opt.get)
      }
    def read(inp: ObjectDataInput): O =
      if (inp.readByte == 0) {
        None.asInstanceOf[O]
      } else {
        new Some(inp.readObject).asInstanceOf[O]
      }
  }
  final class TrySerializer[T <: Try[_]: ClassTag] extends StreamSerializer[T] {
    def write(out: ObjectDataOutput, tr: T): Unit = tr match {
      case Success(obj) =>
        out.write(1)
        out.writeObject(obj)
      case Failure(th) =>
        out.write(0)
        out.writeObject(th)
    }
    def read(inp: ObjectDataInput): T =
      if (inp.readByte == 1) {
        new Success(inp.readObject).asInstanceOf[T]
      } else {
        new Failure(inp.readObject).asInstanceOf[T]
      }
  }

  final class EitherSerializer[E <: Either[_, _]: ClassTag] extends StreamSerializer[E] {
    def write(out: ObjectDataOutput, e: E): Unit = e match {
      case Right(obj) =>
        out.write(1)
        out.writeObject(obj)
      case Left(obj) =>
        out.write(0)
        out.writeObject(obj)
    }
    def read(inp: ObjectDataInput): E =
      if (inp.readByte == 1) {
        new Right(inp.readObject).asInstanceOf[E]
      } else {
        new Left(inp.readObject).asInstanceOf[E]
      }
  }

  final class ListSerializer[L <: List[_]: ClassTag] extends StreamSerializer[L] {
    def write(out: ObjectDataOutput, l: L): Unit = {
      var len = 0
      val reverse = l.foldLeft(List.empty[Any]) {
        case (reverse, e) =>
          len += 1
          e :: reverse
      }
      out.writeInt(len)
      reverse.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): L = {
      val len = inp.readInt
        @tailrec
        def buildList(list: List[Any] = Nil, idx: Int = 0): List[Any] = {
          if (idx < len) {
            buildList(inp.readObject[Any] :: list, idx + 1)
          } else list
        }
      buildList().asInstanceOf[L]
    }
  }

  @tailrec private def readJMap[M <: java.util.Map[Any, Any]](size: Int, map: M, inp: ObjectDataInput, idx: Int = 0): M = {
    if (idx < size) {
      map.put(inp.readObject, inp.readObject)
      readJMap(size, map, inp, idx + 1)
    } else map
  }
  private def writeJMap(map: java.util.Map[Any, Any], out: ObjectDataOutput): Unit = {
    val iter = map.entrySet.iterator
    while (iter.hasNext) {
      val entry = iter.next
      out.writeObject(entry.getKey)
      out.writeObject(entry.getValue)
    }
  }
  @tailrec private def readMutable[E, C](size: Int, builder: Builder[E, C], next: () => E, idx: Int = 0): C = {
    if (idx < size) {
      readMutable(size, builder += next(), next, idx + 1)
    } else builder.result()
  }
  @tailrec private def readIMap[M <: collection.immutable.Map[Any, Any]](size: Int, map: M, inp: ObjectDataInput, idx: Int = 0): M = {
    if (idx < size) {
      readIMap(size, (map + Tuple2Ser.read(inp)).asInstanceOf[M], inp, idx + 1)
    } else map
  }
  private def writeSMap(map: collection.Map[Any, Any], out: ObjectDataOutput): Unit = {
    map.foreach(Tuple2Ser.write(out, _))
  }

}
