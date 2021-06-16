package com.hazelcast.Scala

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import com.hazelcast.core._
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.partition.{PartitionLostEvent, ReplicaMigrationEvent}
import com.hazelcast.transaction.TransactionOptions
import com.hazelcast.transaction.TransactionOptions.TransactionType
import com.hazelcast.transaction.TransactionalTask
import com.hazelcast.transaction.TransactionalTaskContext
import com.hazelcast.nio.serialization.ByteArraySerializer
import com.hazelcast.Scala.serialization.ByteArrayInterceptor
import com.hazelcast.cluster.{InitialMembershipEvent, Member}
import com.hazelcast.map.IMap
import scala.reflect.ClassTag
import com.hazelcast.transaction.TransactionContext

private object HzHazelcastInstance {
  private[this] val DefaultTxnOpts = TransactionOptions.getDefault
  private val DefaultTxnType = DefaultTxnOpts.getTransactionType match {
    case TransactionType.ONE_PHASE => OnePhase
    case TransactionType.TWO_PHASE => TwoPhase(DefaultTxnOpts.getDurability)
  }
  private val DefaultTxnTimeout = FiniteDuration(TransactionOptions.getDefault.getTimeoutMillis, TimeUnit.MILLISECONDS)
}

final class HzHazelcastInstance(hz: HazelcastInstance) extends MemberEventSubscription {
  import HzHazelcastInstance._

  type ESR = ListenerRegistration

  private[Scala] def groupByPartitionId[K](keys: collection.Set[K]): Map[Int, collection.Set[K]] = {
    val ps = hz.getPartitionService
    keys.groupBy(ps.getPartition(_).getPartitionId)
  }
  private[Scala] def groupByMember[K](keys: collection.Set[K]): Map[Member, collection.Set[K]] = {
    val ps = hz.getPartitionService
    keys.groupBy(ps.getPartition(_).getOwner)
  }

  private[Scala] def queryPool(): IExecutorService = hz.getExecutorService("hz:query")

  def onDistributedObjectEvent(runOn: ExecutionContext = null)(listener: PartialFunction[DistributedObjectChange, Unit]): ESR = {
    val regId = hz addDistributedObjectListener EventSubscription.asDistributedObjectListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel() = hz removeDistributedObjectListener regId
    }
  }

  def onLifecycleStateChange(runOn: ExecutionContext = null)(listener: PartialFunction[LifecycleState, Unit]): ESR = {
    val service = hz.getLifecycleService
    val regId = service addLifecycleListener EventSubscription.asLifecycleListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel() = service removeLifecycleListener regId
    }
  }

  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ESR = {
    hz.getPartitionService.onPartitionLost(runOn)(listener)
  }
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[ReplicaMigrationEvent, Unit]): ESR = {
    hz.getPartitionService.onMigration(runOn)(listener)
  }
  def onClient(runOn: ExecutionContext = null)(listener: PartialFunction[ClientEvent, Unit]): ESR = {
    hz.getClientService.onClient(runOn)(listener)
  }
  type MER = (ESR, Future[InitialMembershipEvent])
  def onMemberChange(runOn: ExecutionContext = null)(listener: PartialFunction[MemberEvent, Unit]): MER = {
    hz.getCluster.onMemberChange(runOn)(listener)
  }

  private def toTxnOpts(txnType: TxnType, timeout: FiniteDuration) = {
    val opts = new TransactionOptions().setTimeout(timeout.length, timeout.unit)
    txnType match {
      case OnePhase =>
        opts.setTransactionType(TransactionType.ONE_PHASE)
      case TwoPhase(durability) =>
        opts.setTransactionType(TransactionType.TWO_PHASE).setDurability(durability)
    }
  }

  /**
    * Execute transaction. Commit or rollback is done implicitly.
    * @param txnType Type of transaction
    * @param timeout Transaction timeout
    */
  def transaction[T](
    txnType: TxnType = DefaultTxnType,
    timeout: FiniteDuration = DefaultTxnTimeout)(thunk: TransactionalTaskContext => T): T = {
    transaction(toTxnOpts(txnType, timeout))(thunk)
  }
  def transaction[T](opts: TransactionOptions)(thunk: TransactionalTaskContext => T): T = {
    val task = new TransactionalTask[T] {
      def execute(ctx: TransactionalTaskContext) = thunk(ctx)
    }
    if (opts == null) {
      hz.executeTransaction(task)
    } else {
      hz.executeTransaction(opts, task)
    }
  }
  /**
    * Begin transaction. Commit or rollback must be done explicitly.
    * @param txnType Type of transaction
    * @param timeout Transaction timeout
    */
  def beginTransaction[T](
    txnType: TxnType = DefaultTxnType,
    timeout: FiniteDuration = DefaultTxnTimeout)(thunk: TransactionContext => T): T = {
    beginTransaction(toTxnOpts(txnType, timeout))(thunk)
  }
  def beginTransaction[T](opts: TransactionOptions)(thunk: TransactionContext => T): T = {
    if (opts == null) {
      val ctx = hz.newTransactionContext()
      ctx.beginTransaction()
      thunk(ctx)
    } else {
      val ctx = hz.newTransactionContext(opts)
      ctx.beginTransaction()
      thunk(ctx)
    }
  }

  def isClient: Boolean = {
    val cluster = hz.getCluster
    try {
      cluster.getLocalMember == null
    } catch {
      case _: UnsupportedOperationException => true
    }
  }

  def userCtx: UserContext = new UserContext(hz.getUserContext)

  /**
    *  Experimental, and of questionable value.
    *  Main purpose is to avoid registering a serializer
    *  in the configuration, but instead supply one here
    *  and using an interceptor to convert to/from byte array.
    *  NOTICE: This is marked as deprecated to indicate that
    *  it may be removed in a future version, pending feedback.
    */
  def getBinaryMap[K, V <: AnyRef: ByteArraySerializer: ClassTag](name: String): IMap[K, V] = {
    val imap = hz.getMap[K, Array[Byte]](name)
    imap.addInterceptor(new ByteArrayInterceptor[V])
    imap.asInstanceOf[IMap[K, V]]
  }

  def getExecutorService(): IExecutorService = hz.getExecutorService("default")
}
