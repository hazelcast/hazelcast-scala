package com.hazelcast.Scala.serialization

import java.math.{ MathContext, RoundingMode }
import java.util.{ Comparator, UUID }
import scala.annotation.tailrec
import scala.collection.mutable.Builder
import scala.reflect.ClassTag
import scala.util.{ Either, Failure, Left, Right, Success, Try }
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import scala.collection.immutable.Nil
import scala.runtime.IntRef
import scala.runtime.LongRef
import scala.runtime.DoubleRef
import scala.runtime.FloatRef
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate

object DefaultSerializers extends SerializerEnum(-987654321) {

  val SomeSer = new OptionSerializer[Some[_]]
  val NoneSer = new OptionSerializer[None.type]

  val SuccessSer = new TrySerializer[Success[_]]
  val FailureSer = new TrySerializer[Failure[_]]

  val LeftSer = new EitherSerializer[Left[_, _]]
  val RightSer = new EitherSerializer[Right[_, _]]

  val NilSer = new ListSerializer[Nil.type]
  val NEListSer = new ListSerializer[::[_]]

  val IMapEntrySer = new JMapEntrySerializer[ImmutableEntry[_, _]]((key, value) => new ImmutableEntry(key, value))
  val MMapEntrySer = new JMapEntrySerializer[MutableEntry[_, _]]((key, value) => new MutableEntry(key, value))
  val UMapEntrySer = new JMapEntrySerializer[java.util.Map.Entry[_, _]]((key, value) => new ImmutableEntry(key, value))

  val InlineAggregatorSer = new StreamSerializer[InlineAggregator[_, _]] {
    def write(out: ObjectDataOutput, agg: InlineAggregator[_, _]): Unit = {
      out.writeObject(agg.init)
      out.writeObject(agg.seqop)
      out.writeObject(agg.combop)
    }
    def read(inp: ObjectDataInput): InlineAggregator[_, _] = {
      val init = inp.readObject[() => Any]
      val seqop = inp.readObject[(Any, Any) => Any]
      val combop = inp.readObject[(Any, Any) => Any]
      new InlineAggregator[Any, Any](init, seqop, combop)
    }
  }
  val InlineUnitAggregatorSer = new StreamSerializer[InlineUnitAggregator[_, _]] {
    def write(out: ObjectDataOutput, agg: InlineUnitAggregator[_, _]): Unit = {
      out.writeObject(agg.init)
      out.writeObject(agg.seqop)
      out.writeObject(agg.combop)
    }
    def read(inp: ObjectDataInput): InlineUnitAggregator[_, _] = {
      val init = inp.readObject[() => Any]
      val seqop = inp.readObject[(Any, Any) => Any]
      val combop = inp.readObject[(Any, Any) => Any]
      new InlineUnitAggregator[Any, Any](init, seqop, combop)
    }
  }

  val InlineSavingAggregatorSer = new StreamSerializer[InlineSavingAggregator[_, _, _]] {
    def write(out: ObjectDataOutput, agg: InlineSavingAggregator[_, _, _]): Unit = {
      out.writeUTF(agg.mapName)
      out.writeObject(agg.mapKey)
      out.writeObject(agg.init)
      out.writeObject(agg.seqop)
      out.writeObject(agg.combop)
    }
    def read(inp: ObjectDataInput): InlineSavingAggregator[_, _, _] = {
      val mapName = inp.readUTF
      val mapKey = inp.readObject[Any]
      val init = inp.readObject[() => Any]
      val seqop = inp.readObject[(Any, Any) => Any]
      val combop = inp.readObject[(Any, Any) => Any]
      new InlineSavingAggregator[Any, Any, Any](mapName, mapKey, init, seqop, combop)
    }
  }
  val InlineSavingGroupAggregatorSer = new StreamSerializer[InlineSavingGroupAggregator[_, _, _]] {
    def write(out: ObjectDataOutput, agg: InlineSavingGroupAggregator[_, _, _]): Unit = {
      out.writeUTF(agg.mapName)
      InlineUnitAggregatorSer.write(out, agg.unitAggr)
    }
    def read(inp: ObjectDataInput): InlineSavingGroupAggregator[_, _, _] = {
      val mapName = inp.readUTF
      val unitAggr = InlineUnitAggregatorSer.read(inp)
      new InlineSavingGroupAggregator(mapName, unitAggr)
    }
  }
  val TaskSer = new StreamSerializer[Task[_]] {
    def write(out: ObjectDataOutput, task: Task[_]): Unit = {
      out.writeObject(task.thunk)
    }
    def read(inp: ObjectDataInput): Task[_] = {
      val thunk = inp.readObject[() => Any]
      new Task(thunk)
    }
  }
  val InstanceAwareTaskSer = new StreamSerializer[InstanceAwareTask[_]] {
    def write(out: ObjectDataOutput, task: InstanceAwareTask[_]): Unit = {
      out.writeObject(task.thunk)
    }
    def read(inp: ObjectDataInput): InstanceAwareTask[_] = {
      val thunk = inp.readObject[HazelcastInstance => Any]
      new InstanceAwareTask(thunk)
    }
  }
  val AggrMapDDSTaskSer = new StreamSerializer[dds.AggrMapDDSTask[_, _, _]] {
    type Task = dds.AggrMapDDSTask[_, _, _]
    def write(out: ObjectDataOutput, task: Task): Unit = {
      out.writeUTF(task.mapName)
      out.writeObject(task.predicate)
      out.writeObject(task.aggr)
      out.writeObject(task.pipe)
      out.writeObject(task.keysByMemberId)
    }
    def read(inp: ObjectDataInput): Task = {
      val mapName = inp.readUTF
      val predicate = inp.readObject[Option[Predicate[_, _]]]
      val aggr = inp.readObject[Aggregator[Any, _] { type W = Any }]
      val pipe = inp.readObject[Pipe[_]]
      val keysByMemberId = inp.readObject[Map[String, collection.Set[Any]]]
      new dds.AggrMapDDSTask[Any, Any, Any](aggr, mapName, keysByMemberId, predicate, pipe)
    }
  }

  val ObjArraySer = new StreamSerializer[Array[Object]] {
    def write(out: ObjectDataOutput, arr: Array[Object]) {
      out.writeInt(arr.length)
      var idx = 0
      while (idx < arr.length) {
        out.writeObject(arr(idx))
        idx += 1
      }
    }
    def read(inp: ObjectDataInput): Array[Object] = {
      val arr = new Array[Object](inp.readInt)
      var idx = 0
      while (idx < arr.length) {
        arr(idx) = inp.readObject
        idx += 1
      }
      arr
    }
  }

  val IntRefSer = new StreamSerializer[IntRef] {
    def write(out: ObjectDataOutput, ref: IntRef) = out.writeInt(ref.elem)
    def read(inp: ObjectDataInput): IntRef = new IntRef(inp.readInt)
  }
  val LongRefSer = new StreamSerializer[LongRef] {
    def write(out: ObjectDataOutput, ref: LongRef) = out.writeLong(ref.elem)
    def read(inp: ObjectDataInput): LongRef = new LongRef(inp.readLong)
  }
  val DoubleRefSer = new StreamSerializer[DoubleRef] {
    def write(out: ObjectDataOutput, ref: DoubleRef) = out.writeDouble(ref.elem)
    def read(inp: ObjectDataInput): DoubleRef = new DoubleRef(inp.readDouble)
  }
  val FloatRefSer = new StreamSerializer[FloatRef] {
    def write(out: ObjectDataOutput, ref: FloatRef) = out.writeFloat(ref.elem)
    def read(inp: ObjectDataInput): FloatRef = new FloatRef(inp.readFloat)
  }

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
    def write(out: ObjectDataOutput, map: IHashMap): Unit = IMapSer.write(out, map)
    def read(inp: ObjectDataInput): IHashMap = {
      val len = inp.readInt()
      readIMap(len, new IHashMap, inp)
    }
  }
  private type IMap = collection.immutable.Map[Any, Any]
  val IMapSer: StreamSerializer[IMap] = new StreamSerializer[IMap] {
    def write(out: ObjectDataOutput, map: IMap): Unit = {
      out.writeInt(map.size)
      writeSMap(map, out)
    }
    def read(inp: ObjectDataInput): IMap = IHashMapSer.read(inp)
  }
  private type ITreeMap = collection.immutable.TreeMap[Any, Any]
  val ITreeMapSer = new StreamSerializer[ITreeMap] {
    def write(out: ObjectDataOutput, map: ITreeMap): Unit = ISortedMapSer.write(out, map)
    def read(inp: ObjectDataInput): ITreeMap = {
      val ord = inp.readObject[Ordering[Any]]
      val len = inp.readInt()
      readIMap(len, new ITreeMap()(ord), inp)
    }
  }
  private type ISortedMap = collection.immutable.SortedMap[Any, Any]
  val ISortedMapSer: StreamSerializer[ISortedMap] = new StreamSerializer[ISortedMap] {
    def write(out: ObjectDataOutput, map: ISortedMap): Unit = {
      out.writeObject(map.ordering)
      out.writeInt(map.size)
      writeSMap(map, out)
    }
    def read(inp: ObjectDataInput): ISortedMap = ITreeMapSer.read(inp)
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
      build(len, builder, () => Tuple2Ser.read(inp))
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
      build(len, builder, () => Tuple2Ser.read(inp))
    }
  }
  private type MMap = collection.mutable.Map[Any, Any]
  val MMapSer = new StreamSerializer[MMap] {
    def write(out: ObjectDataOutput, m: MMap): Unit = {
      out.writeInt(m.size)
      writeSMap(m, out)
    }
    def read(inp: ObjectDataInput): MMap = MHashMapSer.read(inp)
  }
  private type AMap = collection.Map[Any, Any]
  val AMapSer = new StreamSerializer[AMap] {
    def write(out: ObjectDataOutput, m: AMap): Unit = {
      out.writeInt(m.size)
      writeSMap(m, out)
    }
    def read(inp: ObjectDataInput): AMap = {
      val size = inp.readInt()
      readIMap(size, Map.empty, inp)
    }
  }
  private type MTreeSet = collection.mutable.TreeSet[Any]
  val MTreeSetSer = new StreamSerializer[MTreeSet] {
    def write(out: ObjectDataOutput, s: MTreeSet): Unit = MSortedSetSer.write(out, s)
    def read(inp: ObjectDataInput): MTreeSet = {
      val ord = inp.readObject[Ordering[Any]]
      val len = inp.readInt()
      val builder = collection.mutable.TreeSet.newBuilder[Any](ord)
      builder.sizeHint(len)
      build(len, builder, () => inp.readObject[Any])
    }
  }
  private type MSortedSet = collection.mutable.SortedSet[Any]
  val MSortedSetSer: StreamSerializer[MSortedSet] = new StreamSerializer[MSortedSet] {
    def write(out: ObjectDataOutput, s: MSortedSet): Unit = {
      out.writeObject(s.ordering)
      out.writeInt(s.size)
      s.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): MSortedSet = MTreeSetSer.read(inp)
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
      build(size, builder, () => inp.readObject[Any])
    }
  }
  private type ITreeSet = collection.immutable.TreeSet[Any]
  val ITreeSetSer = new StreamSerializer[ITreeSet] {
    def write(out: ObjectDataOutput, s: ITreeSet): Unit = ISortedSetSer.write(out, s)
    def read(inp: ObjectDataInput): ITreeSet = {
      val ord = inp.readObject[Ordering[Any]]
      val len = inp.readInt()
      val builder = collection.immutable.TreeSet.newBuilder[Any](ord)
      builder.sizeHint(len)
      build(len, builder, () => inp.readObject[Any])
    }
  }
  private type IHashSet = collection.immutable.HashSet[Any]
  val IHashSetSer = new StreamSerializer[IHashSet] {
    def write(out: ObjectDataOutput, s: IHashSet): Unit = {
      out.writeInt(s.size)
      s.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): IHashSet = {
      val len = inp.readInt()
      val builder = collection.immutable.HashSet.newBuilder[Any]
      builder.sizeHint(len)
      build(len, builder, () => inp.readObject[Any])
    }
  }
  private type ISortedSet = collection.immutable.SortedSet[Any]
  val ISortedSetSer: StreamSerializer[ISortedSet] = new StreamSerializer[ISortedSet] {
    def write(out: ObjectDataOutput, s: ISortedSet): Unit = {
      out.writeObject(s.ordering)
      out.writeInt(s.size)
      s.foreach(out.writeObject)
    }
    def read(inp: ObjectDataInput): ISortedSet = ITreeSetSer.read(inp)
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

  final class JMapEntrySerializer[E <: java.util.Map.Entry[_, _]: ClassTag](ctor: (Any, Any) => E) extends StreamSerializer[E] {
    def write(out: ObjectDataOutput, e: E): Unit = {
      out.writeObject(e.getKey)
      out.writeObject(e.getValue)
    }
    def read(inp: ObjectDataInput): E = ctor(inp.readObject, inp.readObject)
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
      new MathContext(inp.readInt, inp.readObject[RoundingMode])
  }
  val BigDecSer = new StreamSerializer[BigDecimal] {
    def write(out: ObjectDataOutput, bd: BigDecimal): Unit = {
      out.writeObject(bd.underlying)
      MathCtxSer.write(out, bd.mc)
    }
    def read(inp: ObjectDataInput): BigDecimal =
      new BigDecimal(inp.readObject[java.math.BigDecimal], MathCtxSer.read(inp))
  }
  val BigIntSer = new StreamSerializer[BigInt] {
    def write(out: ObjectDataOutput, bi: BigInt): Unit = out.writeObject(bi.underlying)
    def read(inp: ObjectDataInput): BigInt = new BigInt(inp.readObject[java.math.BigInteger])
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
  @tailrec private def build[E, C](size: Int, builder: Builder[E, C], next: () => E, idx: Int = 0): C = {
    if (idx < size) {
      build(size, builder += next(), next, idx + 1)
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
