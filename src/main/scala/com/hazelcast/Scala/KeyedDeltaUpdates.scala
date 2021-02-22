package com.hazelcast.Scala

import java.util.Map.Entry
import concurrent.Future
import com.hazelcast.core._
import com.hazelcast.map.IMap

private[Scala] trait KeyedDeltaUpdates[K, V] {
  type UpdateR[T]

  def upsertAndGet(key: K, insertIfMissing: V, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[V]

  def updateAndGet(key: K, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[Option[V]]
  def updateAndGet(key: K, initIfMissing: V)(update: V => V): UpdateR[V]

  def updateAndGetIf(cond: V => Boolean, key: K, runOn: IExecutorService = null)(update: V => V): UpdateR[Option[V]]

  def upsert(key: K, insertIfMissing: V, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[UpsertResult]

  /**
   * Update value, if present.
   * @return `true` if present (thus updated), `false` if unknown key
   */
  def update(key: K, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[Boolean]
  /**
   * Update value, initialize if missing.
   * NOTE: The `update` function will always run, also on the initial value.
   * @return `true` if present, `false` if unknown key
   */
  def update(key: K, initIfMissing: V)(update: V => V): UpdateR[Boolean]

  def updateIf(cond: V => Boolean, key: K, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[Boolean]
  def getAndUpsert(key: K, insertIfMissing: V, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[Option[V]]
  def getAndUpdate(key: K, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[Option[V]]
  def getAndUpdateIf(cond: V => Boolean, key: K, runOn: IExecutorService = null)(updateIfPresent: V => V): UpdateR[Option[(V, Boolean)]]

}

private[Scala] object KeyedDeltaUpdates {
  final class UpsertEP[V](val insertIfMissing: V, val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, UpsertResult] {
    def onEntry(entry: Entry[Any, V]): UpsertResult =
      entry.value match {
        case null =>
          entry.value = insertIfMissing
          WasInserted
        case value =>
          entry.value = updateIfPresent(value)
          WasUpdated
      }
  }
  final class UpsertAndGetEP[V](val insertIfMissing: V, val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, V] {
    def onEntry(entry: Entry[Any, V]): V = {
      entry.value match {
        case null =>
          entry.value = insertIfMissing
          null.asInstanceOf[V] // Return `null` because we already have value locally
        case value =>
          entry.value = updateIfPresent(value)
          entry.value
      }
    }
  }
  final class UpdateIfEP[V](val cond: V => Boolean, val updateIfPresent: V => V)
    extends SingleEntryCallbackUpdater[Any, V, Boolean] {
    def onEntry(entry: Entry[Any, V]): Boolean = {
      entry.value match {
        case null => false
        case value if cond(value) =>
          entry.value = updateIfPresent(value)
          true
        case _ => false
      }
    }
  }
  final class UpdateEP[V](val initIfMissing: V, val updateIfPresent: V => V)
    extends SingleEntryCallbackUpdater[Any, V, Boolean] {
    def onEntry(entry: Entry[Any, V]): Boolean = {
      val isPresent = entry.value != null
      (if (isPresent) entry.value else initIfMissing) match {
        case null => false
        case value =>
          entry.value = updateIfPresent(value)
          isPresent
      }
    }
  }
  final class UpdateAndGetEP[V](val cond: V => Boolean, val updateIfPresent: V => V, val initIfMissing: V)
    extends SingleEntryCallbackUpdater[Any, V, V] {
    def onEntry(entry: Entry[Any, V]): V =
      (if (entry.value == null) initIfMissing else entry.value) match {
        case null => null.asInstanceOf[V]
        case value if cond(value) =>
          entry.value = updateIfPresent(value)
          entry.value
        case _ => null.asInstanceOf[V]
      }
  }
  final class GetAndUpsertEP[V](val insertIfMissing: V, val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, V] {
    def onEntry(entry: Entry[Any, V]): V = {
      entry.value match {
        case null =>
          entry.value = insertIfMissing
          null.asInstanceOf[V]
        case value =>
          entry.value = updateIfPresent(value)
          value
      }
    }
  }
  final class GetAndUpdateEP[V](val cond: V => Boolean, val updateIfPresent: V => V)
      extends SingleEntryCallbackUpdater[Any, V, (V, Boolean)] {
    def onEntry(entry: Entry[Any, V]): (V, Boolean) =
      entry.value match {
        case null => null
        case value if cond(value) =>
          entry.value = updateIfPresent(value)
          value -> true
        case value =>
          value -> false
      }
  }
  object DeltaTask {
    trait Result
    private case object Retry extends Result
    case object UpdateSuccess extends Result
    case object InsertSuccess extends Result
    case object ConditionFailed extends Result
    case object EntryNotFound extends Result
  }
  abstract class DeltaTask[K, V, +R]
      extends (HazelcastInstance => R) {

    import DeltaTask._
    private def NULL = null.asInstanceOf[V]

    def mapName: String
    def key: K
    def partitionId: Int
    def insertIfMissing: Option[V]
    def updateIfPresent: V => V
    def cond: V => Boolean
    def getResult(taskRes: Result, oldVal: V, newVal: V): R

    private def updateExisting(imap: IMap[K, V], oldValue: V): (Result, V) = {
      if (cond(oldValue)) {
        val newValue = updateIfPresent(oldValue)
        if (imap.replace(key, oldValue, newValue)) {
          UpdateSuccess -> newValue
        } else {
          Retry -> NULL
        }
      } else {
        ConditionFailed -> NULL
      }
    }

    def apply(hz: HazelcastInstance): R = process(hz.getMap[K, V](mapName))

    private def process(imap: IMap[K, V], attemptsLeft: Int = 16): R = {
      if (attemptsLeft == 0) {
        throw new HazelcastException(s"Gave up; concurrency too high on key $key in $imap")
      } else {
        val oldValue = imap.getFastIfLocal(key, partitionId)
        if (oldValue == null) {
          insertIfMissing match {
            case None => // Update
              getResult(DeltaTask.EntryNotFound, oldValue, NULL)
            case Some(insertIfMissing) => // Upsert
              imap.putIfAbsent(key, insertIfMissing) match {
                case null => // Inserted
                  getResult(DeltaTask.InsertSuccess, NULL, insertIfMissing)
                case oldValue => // Existing
                  updateExisting(imap, oldValue) match {
                    case (Retry, _) => // Retry
                      process(imap, attemptsLeft - 1)
                    case (status, newValue) =>
                      getResult(status, oldValue, newValue)
                  }
              }

          }
        } else {
          updateExisting(imap, oldValue) match {
            case (Retry, _) => // Retry
              process(imap, attemptsLeft - 1)
            case (status, newValue) =>
              getResult(status, oldValue, newValue)
          }
        }
      }
    }
  }

  final class GetAndUpdateTask[K, V](
    val mapName: String,
    val key: K,
    val partitionId: Int,
    val updateIfPresent: V => V,
    val cond: V => Boolean)
      extends DeltaTask[K, V, Option[(V, Boolean)]] {

    import DeltaTask._
    def insertIfMissing = None

    def getResult(taskRes: Result, oldVal: V, newVal: V) = taskRes match {
      case UpdateSuccess => Some((oldVal, true))
      case ConditionFailed => Some((oldVal, false))
      case EntryNotFound => None
    }
  }
  final class UpdateTask[K, V](
    val mapName: String,
    val key: K,
    val partitionId: Int,
    val updateIfPresent: V => V,
    val cond: V => Boolean)
      extends DeltaTask[K, V, Boolean] {

    import DeltaTask._

    def insertIfMissing = None
    def getResult(taskRes: Result, oldVal: V, newVal: V) = taskRes match {
      case UpdateSuccess => true
      case ConditionFailed | EntryNotFound => false
    }
  }

  final class UpdateAndGetTask[K, V](
    val mapName: String,
    val key: K,
    val partitionId: Int,
    val updateIfPresent: V => V,
    val cond: V => Boolean)
      extends DeltaTask[K, V, Option[V]] {

    import DeltaTask._

    def insertIfMissing = None
    def getResult(taskRes: Result, oldVal: V, newVal: V) = taskRes match {
      case UpdateSuccess => Some(newVal)
      case ConditionFailed | EntryNotFound => None
    }
  }

  final class GetAndUpsertTask[K, V](
    val mapName: String,
    val key: K,
    val partitionId: Int,
    val insertIfMissing: Some[V],
    val updateIfPresent: V => V)
      extends DeltaTask[K, V, Option[V]] {
    import DeltaTask._
    def cond = TrueFunction[V]
    def getResult(taskRes: Result, oldVal: V, newVal: V) = taskRes match {
      case UpdateSuccess => Some(oldVal)
      case InsertSuccess => None
    }
  }

  final class UpsertAndGetTask[K, V](
    val mapName: String,
    val key: K,
    val partitionId: Int,
    val insertIfMissing: Some[V],
    val updateIfPresent: V => V)
      extends DeltaTask[K, V, V] {
    import DeltaTask._
    def cond = TrueFunction[V]
    def getResult(taskRes: Result, oldVal: V, newVal: V) = taskRes match {
      case UpdateSuccess => newVal
      case InsertSuccess => null.asInstanceOf[V]
    }
  }

  final class UpsertTask[K, V](
    val mapName: String,
    val key: K,
    val partitionId: Int,
    val insertIfMissing: Some[V],
    val updateIfPresent: V => V)
      extends DeltaTask[K, V, UpsertResult] {
    import DeltaTask._
    def cond = TrueFunction[V]
    def getResult(taskRes: Result, oldVal: V, newVal: V) = taskRes match {
      case UpdateSuccess => WasUpdated
      case InsertSuccess => WasInserted
    }
  }
}

private[Scala] trait KeyedIMapDeltaUpdates[K, V]
    extends KeyedDeltaUpdates[K, V] {
  self: HzMap[K, V] =>

  type UpdateR[T] = T

  def upsertAndGet(key: K, insertIfMissing: V, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[V] =
    async.upsertAndGet(key, insertIfMissing, runOn)(updateIfPresent).await

  def updateAndGet(key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Option[V]] =
    async.updateAndGet(key, runOn)(updateIfPresent).await
  def updateAndGet(key: K, initIfMissing: V)(updateIfPresent: V => V): UpdateR[V] =
    async.updateAndGet(key, initIfMissing)(updateIfPresent).await

  def upsert(key: K, insertIfMissing: V, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[UpsertResult] =
    async.upsert(key, insertIfMissing, runOn)(updateIfPresent).await

  def update(key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Boolean] =
    async.update(key, runOn)(updateIfPresent).await
  def update(key: K, initIfMissing: V)(update: V => V): UpdateR[Boolean] =
    async.update(key, initIfMissing)(update).await

  def getAndUpsert(key: K, insertIfMissing: V, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Option[V]] =
    async.getAndUpsert(key, insertIfMissing, runOn)(updateIfPresent).await
  def getAndUpdate(key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Option[V]] =
    async.getAndUpdate(key, runOn)(updateIfPresent).await
  def updateAndGetIf(cond: V => Boolean, key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Option[V]] =
    async.updateAndGetIf(cond, key, runOn)(updateIfPresent).await
  def updateIf(cond: V => Boolean, key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Boolean] =
    async.updateIf(cond, key, runOn)(updateIfPresent).await
  def getAndUpdateIf(cond: V => Boolean, key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Option[(V, Boolean)]] =
    async.getAndUpdateIf(cond, key, runOn)(updateIfPresent).await

}

private[Scala] trait KeyedIMapAsyncDeltaUpdates[K, V] extends KeyedDeltaUpdates[K, V] {
  import com.hazelcast.map.IMap
  import KeyedDeltaUpdates._

  protected def imap: IMap[K, V]

  type UpdateR[T] = Future[T]

  def upsert(key: K, insertIfMissing: V, runOn: IExecutorService)(updateIfPresent: V => V): Future[UpsertResult] = {
    if (runOn == null) {
      val ep = new UpsertEP(insertIfMissing, updateIfPresent)
      val callback = ep.newCallback()
      imap.submitToKey(key, ep, callback) // TODO: This now has too many arguments
      callback.future
    } else {
      val partition = imap.getHZ.getPartitionService.getPartition(key)
      val task = new UpsertTask(imap.getName, key, partition.getPartitionId, Some(insertIfMissing), updateIfPresent)
      runOn.submit(ToMember(partition.getOwner))(task)
    }
  }

  def upsertAndGet(key: K, insertIfMissing: V, runOn: IExecutorService)(updateIfPresent: V => V): Future[V] = {
    if (runOn == null) {
      val ep = new UpsertAndGetEP(insertIfMissing, updateIfPresent)
      val callback = ep.newCallback(insertIfMissing)
      imap.submitToKey(key, ep, callback) // TODO: This now has too many arguments
      callback.future
    } else {
      val partition = imap.getHZ.getPartitionService.getPartition(key)
      val task = new UpsertAndGetTask(imap.getName, key, partition.getPartitionId, Some(insertIfMissing), updateIfPresent)
      runOn.submit(ToMember(partition.getOwner))(task).map {
        case null => insertIfMissing
        case updated => updated
      }(SameThread)
    }
  }

  def updateAndGet(key: K, runOn: IExecutorService)(updateIfPresent: V => V): Future[Option[V]] =
    updateAndGetIf(TrueFunction[V], key, runOn)(updateIfPresent)

  def updateAndGet(key: K, initIfMissing: V)(update: V => V): Future[V] = {
    val ep = new UpdateAndGetEP(TrueFunction[V], update, initIfMissing)
    imap.submitToKey(key, ep) // TODO: This now has too many arguments
  }

  def update(key: K, runOn: IExecutorService)(updateIfPresent: V => V): Future[Boolean] =
    updateIf(TrueFunction[V], key, runOn)(updateIfPresent)
  def update(key: K, initIfMissing: V)(update: V => V): Future[Boolean] = {
    val ep = new UpdateEP(initIfMissing, update)
    val callback = ep.newCallback()
    imap.submitToKey(key, ep, callback) // TODO: This now has too many arguments
    callback.future
  }

  def getAndUpsert(key: K, insertIfMissing: V, runOn: IExecutorService)(updateIfPresent: V => V): Future[Option[V]] = {
    if (runOn == null) {
      val ep = new GetAndUpsertEP(insertIfMissing, updateIfPresent)
      val callback = ep.newCallbackOpt
      imap.submitToKey(key, ep, callback) // TODO: This now has too many arguments
      callback.future
    } else {
      val partition = imap.getHZ.getPartitionService.getPartition(key)
      val task = new GetAndUpsertTask(imap.getName, key, partition.getPartitionId, Some(insertIfMissing), updateIfPresent)
      runOn.submit(ToMember(partition.getOwner))(task)
    }
  }

  def getAndUpdate(key: K, runOn: IExecutorService)(updateIfPresent: V => V): Future[Option[V]] =
    getAndUpdateIf(TrueFunction[V], key, runOn)(updateIfPresent).map(opt => opt.map(_._1))(SameThread)

  def updateAndGetIf(cond: V => Boolean, key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Option[V]] = {
    if (runOn == null) {
      val ep = new UpdateAndGetEP(cond, updateIfPresent, null.asInstanceOf[V])
      val callback = ep.newCallbackOpt
      imap.submitToKey(key, ep, callback) // TODO: This now has too many arguments
      callback.future
    } else {
      val partition = imap.getHZ.getPartitionService.getPartition(key)
      val task = new UpdateAndGetTask(imap.getName, key, partition.getPartitionId, updateIfPresent, cond)
      runOn.submit(ToMember(partition.getOwner))(task)
    }
  }
  def updateIf(cond: V => Boolean, key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Boolean] = {
    if (runOn == null) {
      val ep = new UpdateIfEP(cond, updateIfPresent)
      val callback = ep.newCallback()
      imap.submitToKey(key, ep, callback)
      callback.future
    } else {
      val partition = imap.getHZ.getPartitionService.getPartition(key)
      val task = new UpdateTask(imap.getName, key, partition.getPartitionId, updateIfPresent, cond)
      runOn.submit(ToMember(partition.getOwner))(task)
    }
  }
  def getAndUpdateIf(cond: V => Boolean, key: K, runOn: IExecutorService)(updateIfPresent: V => V): UpdateR[Option[(V, Boolean)]] = {
    if (runOn == null) {
      val ep = new GetAndUpdateEP(cond, updateIfPresent)
      val callback = ep.newCallbackOpt
      imap.submitToKey(key, ep, callback) // TODO: This now has too many arguments
      callback.future
    } else {
      val partition = imap.getHZ.getPartitionService.getPartition(key)
      val task = new GetAndUpdateTask(imap.getName, key, partition.getPartitionId, updateIfPresent, cond)
      runOn.submit(ToMember(partition.getOwner))(task)
    }
  }

}
