package com.hazelcast.Scala.serialization

import java.util.Map.Entry
import java.math.{ MathContext, RoundingMode }
import java.util.{ Comparator, UUID }
import scala.annotation.tailrec
import scala.collection.mutable.Builder
import scala.reflect.ClassTag
import scala.util.{ Failure, Left, Right, Success }
import com.hazelcast.nio.{ ObjectDataInput, ObjectDataOutput }
import scala.collection.immutable.Nil
import scala.runtime.{ IntRef, LongRef, DoubleRef, FloatRef }
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import scala.concurrent.duration._
import Duration.Infinite

object Defaults extends SerializerEnum(-987654321) {

  val TrueFunctionSer = new StreamSerializer[TrueFunction.type] {
    def write(out: ObjectDataOutput, f: TrueFunction.type) = ()
    def read(inp: ObjectDataInput) = TrueFunction
  }

  val SomeSer = new StreamSerializer[Some[Any]] {
    def write(out: ObjectDataOutput, some: Some[Any]) = out writeObject some.value
    def read(inp: ObjectDataInput) = new Some(inp.readObject[Any])
  }
  val NoneSer = new StreamSerializer[None.type] {
    def write(out: ObjectDataOutput, none: None.type) = ()
    def read(inp: ObjectDataInput) = None
  }

  val SuccessSer = new StreamSerializer[Success[Any]] {
    def write(out: ObjectDataOutput, success: Success[Any]) = out.writeObject(success.value)
    def read(inp: ObjectDataInput) = new Success(inp.readObject[Any])
  }
  val FailureSer = new StreamSerializer[Failure[_]] {
    def write(out: ObjectDataOutput, failure: Failure[_]) = out.writeObject(failure.exception)
    def read(inp: ObjectDataInput) = new Failure(inp.readObject[Throwable])
  }

  val LeftSer = new StreamSerializer[Left[Any, _]] {
    def write(out: ObjectDataOutput, left: Left[Any, _]) = out writeObject left.value
    def read(inp: ObjectDataInput) = new Left(inp.readObject[Any])
  }
  val RightSer = new StreamSerializer[Right[_, Any]] {
    def write(out: ObjectDataOutput, right: Right[_, Any]) = out writeObject right.value
    def read(inp: ObjectDataInput) = new Right(inp.readObject[Any])
  }

  val NilSer = new StreamSerializer[Nil.type] {
    def write(out: ObjectDataOutput, nil: Nil.type) = ()
    def read(inp: ObjectDataInput) = Nil
  }
  val NEListSer = new ListSerializer[::[_]]

  val UpdatedUpsertResultSer = new UpsertResultSerializer(WasUpdated)
  val InsertedUpsertResultSer = new UpsertResultSerializer(WasInserted)

  val FiniteDurationSer = new StreamSerializer[FiniteDuration] {
    def write(out: ObjectDataOutput, dur: FiniteDuration): Unit = {
      out.writeLong(dur.length)
      out.writeObject(dur.unit)
    }
    def read(inp: ObjectDataInput) =
      new FiniteDuration(inp.readLong, inp.readObject[TimeUnit])
  }
  val InfiniteDurationSer = new StreamSerializer[Infinite] {
    def write(out: ObjectDataOutput, dur: Infinite): Unit = {
      if (dur eq Duration.Inf) out.writeByte(0)
      else if (dur eq Duration.MinusInf) out.writeByte(1)
      else if (dur eq Duration.Undefined) out.writeByte(2)
      else sys.error(s"New Infinite Duration encountered: $dur (${dur.getClass})")
    }
    def read(inp: ObjectDataInput) = inp.readByte match {
      case 0 => Duration.Inf
      case 1 => Duration.MinusInf
      case 2 => Duration.Undefined
      case b => sys.error(s"Unexpected Infinite duration type: ${b.toInt}")
    }
  }

  val IMapEntrySer = new JMapEntrySerializer[ImmutableEntry[_, _]]((key, value) => new ImmutableEntry(key, value))
  val MMapEntrySer = new JMapEntrySerializer[MutableEntry[_, _]]((key, value) => new MutableEntry(key, value))
  val UMapEntrySer = new JMapEntrySerializer[Entry[_, _]]((key, value) => new ImmutableEntry(key, value))

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
  val RemoteTaskSer = new StreamSerializer[RemoteTask[_]] {
    def write(out: ObjectDataOutput, task: RemoteTask[_]): Unit = {
      out.writeObject(task.thunk)
    }
    def read(inp: ObjectDataInput): RemoteTask[_] = {
      val thunk = inp.readObject[HazelcastInstance => Any]
      new RemoteTask(thunk)
    }
  }
  val ForEachEPSer = new StreamSerializer[HzMap.ForEachEP[_, _, Any]] {
    def write(out: ObjectDataOutput, ep: HzMap.ForEachEP[_, _, Any]): Unit = {
      out.writeObject(ep.getCtx)
      out.writeObject(ep.thunk)
    }
    def read(inp: ObjectDataInput): HzMap.ForEachEP[_, _, Any] = {
      val getCtx = inp.readObject[HazelcastInstance => Any]
      val thunk = inp.readObject[(Any, Any, Any) => Unit]
      new HzMap.ForEachEP(getCtx, thunk)
    }
  }
  val ContextQueryEPSer = new StreamSerializer[HzMap.ContextQueryEP[Any, _, _, Any]] {
    def write(out: ObjectDataOutput, ep: HzMap.ContextQueryEP[Any, _, _, Any]): Unit = {
      out.writeObject(ep.getCtx)
      out.writeObject(ep.mf)
    }
    def read(inp: ObjectDataInput): HzMap.ContextQueryEP[Any, _, _, Any] = {
      val getCtx = inp.readObject[HazelcastInstance => Any]
      val mf = inp.readObject[(Any, Any, Any) => Any]
      new HzMap.ContextQueryEP(getCtx, mf)
    }
  }
  val QueryEPSer = new StreamSerializer[HzMap.QueryEP[Any, Any]] {
    def write(out: ObjectDataOutput, ep: HzMap.QueryEP[Any, Any]): Unit = {
      out.writeObject(ep.mf)
    }
    def read(inp: ObjectDataInput): HzMap.QueryEP[Any, Any] = {
      val mf = inp.readObject[Any => Any]
      new HzMap.QueryEP(mf)
    }
  }
  val GetAllAsEPSer = new StreamSerializer[HzMap.GetAllAsEP[Any, Any, Any]] {
    def write(out: ObjectDataOutput, ep: HzMap.GetAllAsEP[Any, Any, Any]): Unit = {
      out.writeObject(ep.mf)
    }
    def read(inp: ObjectDataInput): HzMap.GetAllAsEP[Any, Any, Any] = {
      val mf = inp.readObject[Any => Any]
      new HzMap.GetAllAsEP(mf)
    }
  }
  val ExecuteEPSer = new StreamSerializer[HzMap.ExecuteEP[_, _, Any]] {
    def write(out: ObjectDataOutput, ep: HzMap.ExecuteEP[_, _, Any]): Unit = {
      out.writeObject(ep.thunk)
    }
    def read(inp: ObjectDataInput): HzMap.ExecuteEP[_, _, Any] = {
      val thunk = inp.readObject[Entry[_, _] => Any]
      new HzMap.ExecuteEP(thunk)
    }
  }
  val ExecuteOptEPSer = new StreamSerializer[HzMap.ExecuteOptEP[_, _, Any]] {
    def write(out: ObjectDataOutput, ep: HzMap.ExecuteOptEP[_, _, Any]): Unit = {
      out.writeObject(ep.thunk)
    }
    def read(inp: ObjectDataInput): HzMap.ExecuteOptEP[_, _, Any] = {
      val thunk = inp.readObject[Entry[_, _] => Any]
      new HzMap.ExecuteOptEP(thunk)
    }
  }
  val ValueUpdaterEPSer = new StreamSerializer[HzMap.ValueUpdaterEP[Any]] {
    def write(out: ObjectDataOutput, ep: HzMap.ValueUpdaterEP[Any]): Unit = {
      out.writeObject(ep.update)
      out.writeObject(ep.returnValue)
    }
    def read(inp: ObjectDataInput) = {
      val update = inp.readObject[Any => Any]
      val returnValue = inp.readObject[Any => Object]
      new HzMap.ValueUpdaterEP(update, returnValue)
    }
  }
  val GetAsEPSer = new StreamSerializer[AsyncMap.GetAsEP[Any, Any]] {
    def write(out: ObjectDataOutput, ep: AsyncMap.GetAsEP[Any, Any]): Unit = {
      out.writeObject(ep.mf)
    }
    def read(inp: ObjectDataInput) = {
      val mf = inp.readObject[Any => Any]
      new AsyncMap.GetAsEP(mf)
    }
  }
  val ContextGetAsEPSer = new StreamSerializer[AsyncMap.ContextGetAsEP[Any, _, Any]] {
    def write(out: ObjectDataOutput, ep: AsyncMap.ContextGetAsEP[Any, _, Any]): Unit = {
      out.writeObject(ep.getCtx)
      out.writeObject(ep.mf)
    }
    def read(inp: ObjectDataInput): AsyncMap.ContextGetAsEP[Any, _, Any] = {
      val getCtx = inp.readObject[HazelcastInstance => Any]
      val mf = inp.readObject[(Any, Any) => Any]
      new AsyncMap.ContextGetAsEP(getCtx, mf)
    }
  }
  val PutIfAbsentEPSer = new StreamSerializer[AsyncMap.PutIfAbsentEP[Any]] {
    def write(out: ObjectDataOutput, ep: AsyncMap.PutIfAbsentEP[Any]): Unit = {
      out.writeObject(ep.putIfAbsent)
    }
    def read(inp: ObjectDataInput) = {
      val value = inp.readObject[Any]
      new AsyncMap.PutIfAbsentEP(value)
    }
  }
  val TTLPutIfAbsentEPSer = new StreamSerializer[AsyncMap.TTLPutIfAbsentEP[Any]] {
    def write(out: ObjectDataOutput, ep: AsyncMap.TTLPutIfAbsentEP[Any]): Unit = {
      out.writeUTF(ep.mapName)
      out.writeObject(ep.putIfAbsent)
      out.writeLong(ep.ttl)
      out.writeObject(ep.unit)
    }
    def read(inp: ObjectDataInput) = {
      new AsyncMap.TTLPutIfAbsentEP(inp.readUTF, inp.readObject[Any], inp.readLong, inp.readObject[TimeUnit])
    }
  }
  val SetIfAbsentEPSer = new StreamSerializer[AsyncMap.SetIfAbsentEP[Any]] {
    def write(out: ObjectDataOutput, ep: AsyncMap.SetIfAbsentEP[Any]): Unit = {
      out.writeObject(ep.putIfAbsent)
    }
    def read(inp: ObjectDataInput) = {
      val value = inp.readObject[Any]
      new AsyncMap.SetIfAbsentEP(value)
    }
  }
  val TTLSetIfAbsentEPSer = new StreamSerializer[AsyncMap.TTLSetIfAbsentEP[Any]] {
    def write(out: ObjectDataOutput, ep: AsyncMap.TTLSetIfAbsentEP[Any]): Unit = {
      out.writeUTF(ep.mapName)
      out.writeObject(ep.putIfAbsent)
      out.writeLong(ep.ttl)
      out.writeObject(ep.unit)
    }
    def read(inp: ObjectDataInput) = {
      new AsyncMap.TTLSetIfAbsentEP(inp.readUTF, inp.readObject[Any], inp.readLong, inp.readObject[TimeUnit])
    }
  }
  val UpsertEPSer = new StreamSerializer[KeyedDeltaUpdates.UpsertEP[Any]] {
    type EP = KeyedDeltaUpdates.UpsertEP[Any]
    def write(out: ObjectDataOutput, ep: EP): Unit = {
      out.writeObject(ep.insertIfMissing)
      out.writeObject(ep.updateIfPresent)
    }
    def read(inp: ObjectDataInput): EP = {
      val insert = inp.readObject[Any]
      val update = inp.readObject[Any => Any]
      new EP(insert, update)
    }
  }
  val UpsertAndGetEPSer = new StreamSerializer[KeyedDeltaUpdates.UpsertAndGetEP[Any]] {
    type EP = KeyedDeltaUpdates.UpsertAndGetEP[Any]
    def write(out: ObjectDataOutput, ep: EP): Unit = {
      out.writeObject(ep.insertIfMissing)
      out.writeObject(ep.updateIfPresent)
    }
    def read(inp: ObjectDataInput): EP = {
      val insert = inp.readObject[Any]
      val update = inp.readObject[Any => Any]
      new EP(insert, update)
    }
  }
  val GetAndUpsertEPSer = new StreamSerializer[KeyedDeltaUpdates.GetAndUpsertEP[Any]] {
    type EP = KeyedDeltaUpdates.GetAndUpsertEP[Any]
    def write(out: ObjectDataOutput, ep: EP): Unit = {
      out.writeObject(ep.insertIfMissing)
      out.writeObject(ep.updateIfPresent)
    }
    def read(inp: ObjectDataInput): EP = {
      val insert = inp.readObject[Any]
      val update = inp.readObject[Any => Any]
      new EP(insert, update)
    }
  }
  val UpdateIfEPSer = new StreamSerializer[KeyedDeltaUpdates.UpdateIfEP[Any]] {
    type EP = KeyedDeltaUpdates.UpdateIfEP[Any]
    def write(out: ObjectDataOutput, ep: EP): Unit = {
      out.writeObject(ep.cond)
      out.writeObject(ep.updateIfPresent)
    }
    def read(inp: ObjectDataInput): EP = {
      val cond = inp.readObject[Any => Boolean]
      val update = inp.readObject[Any => Any]
      new EP(cond, update)
    }
  }
  val UpdateEPSer = new StreamSerializer[KeyedDeltaUpdates.UpdateEP[Any]] {
    type EP = KeyedDeltaUpdates.UpdateEP[Any]
    def write(out: ObjectDataOutput, ep: EP): Unit = {
      out writeObject ep.initIfMissing
      out writeObject ep.updateIfPresent
    }
    def read(inp: ObjectDataInput): EP = {
      val initIfMissing = inp.readObject[Any]
      val update = inp.readObject[Any => Any]
      new EP(initIfMissing, update)
    }
  }
  val UpdateAndGetEPSer = new StreamSerializer[KeyedDeltaUpdates.UpdateAndGetEP[Any]] {
    type EP = KeyedDeltaUpdates.UpdateAndGetEP[Any]
    def write(out: ObjectDataOutput, ep: EP): Unit = {
      out writeObject ep.cond
      out writeObject ep.updateIfPresent
      out writeObject ep.initIfMissing
    }
    def read(inp: ObjectDataInput): EP = {
      val cond = inp.readObject[Any => Boolean]
      val update = inp.readObject[Any => Any]
      val initIfMissing = inp.readObject[Any]
      new EP(cond, update, initIfMissing)
    }
  }
  val GetAndUpdateEPSer = new StreamSerializer[KeyedDeltaUpdates.GetAndUpdateEP[Any]] {
    type EP = KeyedDeltaUpdates.GetAndUpdateEP[Any]
    def write(out: ObjectDataOutput, ep: EP): Unit = {
      out.writeObject(ep.cond)
      out.writeObject(ep.updateIfPresent)
    }
    def read(inp: ObjectDataInput): EP = {
      val cond = inp.readObject[Any => Boolean]
      val update = inp.readObject[Any => Any]
      new EP(cond, update)
    }
  }
  val AggrMapDDSTaskSer = new StreamSerializer[dds.AggrMapDDSTask[_, _, _]] {
    type Task = dds.AggrMapDDSTask[_, _, _]
    def write(out: ObjectDataOutput, task: Task): Unit = {
      out.writeUTF(task.mapName)
      out.writeObject(task.predicate)
      out.writeObject(task.aggr)
      out.writeObject(task.taskSupport)
      out.writeObject(task.pipe)
      out.writeObject(task.keysByMemberId)
    }
    def read(inp: ObjectDataInput): Task = {
      val mapName = inp.readUTF
      val predicate = inp.readObject[Option[Predicate[_, _]]]
      val aggr = inp.readObject[Aggregator[Any, _] { type W = Any }]
      val taskSupport = inp.readObject[Option[UserContext.Key[collection.parallel.TaskSupport]]]
      val pipe = inp.readObject[Pipe[_]]
      val keysByMemberId = inp.readObject[Map[String, collection.Set[Any]]]
      new dds.AggrMapDDSTask[Any, Any, Any](aggr, taskSupport, mapName, keysByMemberId, predicate, pipe)
    }
  }

  val ObjArraySer = new StreamSerializer[Array[Object]] {
    def write(out: ObjectDataOutput, arr: Array[Object]): Unit = {
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
        arr(idx) = inp.readObject[Object]
        idx += 1
      }
      arr
    }
  }

  val GetAndUpdateTaskSer = new DeltaUpdateTaskSer[KeyedDeltaUpdates.GetAndUpdateTask[Any, Any]] {
    def newInstance(mapName: String, key: Any, partitionId: Int, updateIfPresent: Any => Any, cond: Any => Boolean) =
      new KeyedDeltaUpdates.GetAndUpdateTask(mapName, key, partitionId, updateIfPresent, cond)
  }
  val UpdateTaskSer = new DeltaUpdateTaskSer[KeyedDeltaUpdates.UpdateTask[Any, Any]] {
    def newInstance(mapName: String, key: Any, partitionId: Int, updateIfPresent: Any => Any, cond: Any => Boolean) =
      new KeyedDeltaUpdates.UpdateTask(mapName, key, partitionId, updateIfPresent, cond)
  }
  val UpdateAndGetTaskSer = new DeltaUpdateTaskSer[KeyedDeltaUpdates.UpdateAndGetTask[Any, Any]] {
    def newInstance(mapName: String, key: Any, partitionId: Int, updateIfPresent: Any => Any, cond: Any => Boolean) =
      new KeyedDeltaUpdates.UpdateAndGetTask(mapName, key, partitionId, updateIfPresent, cond)
  }
  val GetAndUpsertTaskSer = new DeltaUpsertTaskSer[KeyedDeltaUpdates.GetAndUpsertTask[Any, Any]] {
    def newInstance(mapName: String, key: Any, partitionId: Int, insertIfMissing: Some[Any], updateIfPresent: Any => Any) =
      new KeyedDeltaUpdates.GetAndUpsertTask(mapName, key, partitionId, insertIfMissing, updateIfPresent)
  }
  val UpsertAndGetTaskSer = new DeltaUpsertTaskSer[KeyedDeltaUpdates.UpsertAndGetTask[Any, Any]] {
    def newInstance(mapName: String, key: Any, partitionId: Int, insertIfMissing: Some[Any], updateIfPresent: Any => Any) =
      new KeyedDeltaUpdates.UpsertAndGetTask(mapName, key, partitionId, insertIfMissing, updateIfPresent)
  }
  val UpsertTaskSer = new DeltaUpsertTaskSer[KeyedDeltaUpdates.UpsertTask[Any, Any]] {
    def newInstance(mapName: String, key: Any, partitionId: Int, insertIfMissing: Some[Any], updateIfPresent: Any => Any) =
      new KeyedDeltaUpdates.UpsertTask(mapName, key, partitionId, insertIfMissing, updateIfPresent)
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
    def read(inp: ObjectDataInput): ClassTag[_] = ClassTag(inp.readObject[Class[_]])
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
  private type IIntMap = collection.immutable.IntMap[Any]
  val IntMapSer = new StreamSerializer[IIntMap] {
    def write(out: ObjectDataOutput, map: IIntMap) = {
      out.writeInt(map.size)
      map.foreach {
        case (key, value) =>
          out.writeInt(key)
          out.writeObject(value)
      }
    }
    def read(inp: ObjectDataInput): IIntMap = {
      val size = inp.readInt
      var map = collection.immutable.IntMap[Any]()
      var i = 0
      while (i < size) {
        map = map.updated(inp.readInt, inp.readObject[Any])
        i += 1
      }
      map
    }
  }
  private type ILongMap = collection.immutable.LongMap[Any]
  val LongMapSer = new StreamSerializer[ILongMap] {
    def write(out: ObjectDataOutput, map: ILongMap) = {
      out.writeInt(map.size)
      map.foreach {
        case (key, value) =>
          out.writeLong(key)
          out.writeObject(value)
      }
    }
    def read(inp: ObjectDataInput): ILongMap = {
      val size = inp.readInt
      var map = collection.immutable.LongMap[Any]()
      var i = 0
      while (i < size) {
        map = map.updated(inp.readLong, inp.readObject[Any])
        i += 1
      }
      map
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

  final class JMapEntrySerializer[E <: Entry[_, _]: ClassTag](ctor: (Any, Any) => E) extends StreamSerializer[E] {
    def write(out: ObjectDataOutput, e: E): Unit = {
      out.writeObject(e.getKey)
      out.writeObject(e.getValue)
    }
    def read(inp: ObjectDataInput): E = ctor(inp.readObject[Any], inp.readObject[Any])
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
      (inp.readObject[Any], inp.readObject[Any])
    }
  }
  val Tuple3Ser = new StreamSerializer[Tuple3[_, _, _]] {
    def write(out: ObjectDataOutput, t: Tuple3[_, _, _]): Unit = {
      out.writeObject(t._1)
      out.writeObject(t._2)
      out.writeObject(t._3)
    }
    def read(inp: ObjectDataInput): Tuple3[_, _, _] = {
      (inp.readObject[Any], inp.readObject[Any], inp.readObject[Any])
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
      (inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any])
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
      (inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any])
    }
  }
  val Tuple6Ser = new StreamSerializer[Tuple6[_, _, _, _, _, _]] {
    type Tuple = Tuple6[_, _, _, _, _, _]
    def write(out: ObjectDataOutput, t: Tuple): Unit = {
      out.writeObject(t._1)
      out.writeObject(t._2)
      out.writeObject(t._3)
      out.writeObject(t._4)
      out.writeObject(t._5)
      out.writeObject(t._6)
    }
    def read(inp: ObjectDataInput): Tuple = {
      (inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any])
    }
  }
  val Tuple7Ser = new StreamSerializer[Tuple7[_, _, _, _, _, _, _]] {
    type Tuple = Tuple7[_, _, _, _, _, _, _]
    def write(out: ObjectDataOutput, t: Tuple): Unit = {
      out.writeObject(t._1)
      out.writeObject(t._2)
      out.writeObject(t._3)
      out.writeObject(t._4)
      out.writeObject(t._5)
      out.writeObject(t._6)
      out.writeObject(t._7)
    }
    def read(inp: ObjectDataInput): Tuple = {
      (inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any], inp.readObject[Any])
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

  private[serialization] abstract class DeltaUpsertTaskSer[Task <: KeyedDeltaUpdates.DeltaTask[Any, Any, Any]: ClassTag]
      extends StreamSerializer[Task] {
    def newInstance(mapName: String, key: Any, partitionId: Int, insertIfMissing: Some[Any], updateIfPresent: Any => Any): Task
    final def write(out: ObjectDataOutput, task: Task): Unit = {
      out.writeUTF(task.mapName)
      out.writeObject(task.key)
      out.writeInt(task.partitionId)
      out.writeObject(task.insertIfMissing.get)
      out.writeObject(task.updateIfPresent)
    }
    final def read(inp: ObjectDataInput) = {
      val mapName = inp.readUTF()
      val key = inp.readObject[Any]
      val parId = inp.readInt
      val insertIfMissing = Some(inp.readObject[Any])
      val updateIfPresent = inp.readObject[Any => Any]
      newInstance(mapName, key, parId, insertIfMissing, updateIfPresent)
    }
  }
  private[serialization] abstract class DeltaUpdateTaskSer[Task <: KeyedDeltaUpdates.DeltaTask[Any, Any, Any]: ClassTag]
      extends StreamSerializer[Task] {
    def newInstance(mapName: String, key: Any, partitionId: Int, updateIfPresent: Any => Any, cond: Any => Boolean): Task
    def write(out: ObjectDataOutput, task: Task): Unit = {
      out.writeUTF(task.mapName)
      out.writeObject(task.key)
      out.writeInt(task.partitionId)
      out.writeObject(task.updateIfPresent)
      out.writeObject(task.cond)
    }
    def read(inp: ObjectDataInput) = {
      val mapName = inp.readUTF()
      val key = inp.readObject[Any]
      val parId = inp.readInt
      val updateIfPresent = inp.readObject[Any => Any]
      val cond = inp.readObject[Any => Boolean]
      newInstance(mapName, key, parId, updateIfPresent, cond)
    }
  }

  private[serialization] final class UpsertResultSerializer[UR <: UpsertResult: ClassTag](ur: UR) extends StreamSerializer[UR] {
    def write(out: ObjectDataOutput, ur: UR): Unit = ()
    def read(inp: ObjectDataInput): UR = ur
  }

  private[serialization] final class ListSerializer[L <: List[_]: ClassTag] extends StreamSerializer[L] {
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
      map.put(inp.readObject[Any], inp.readObject[Any])
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
