package com.hazelcast.Scala.xa

import scala.util.control.NonFatal

import com.hazelcast.core._
import com.hazelcast.transaction.TransactionalTaskContext

import javax.transaction.TransactionManager
import javax.transaction.xa.XAResource

class XAHazelcastInstance(private val hz: HazelcastInstance) extends AnyVal {
  def transaction[T](txnMgr: TransactionManager, resources: XAResource*)(thunk: TransactionalTaskContext => T): T = {
    txnMgr.begin()
    val txn = txnMgr.getTransaction
    val hzResource = hz.getXAResource()
    try {
      (hzResource +: resources).foreach(txn.enlistResource)
      val result = thunk(hzResource.getTransactionContext)
      (hzResource +: resources).foreach(txn.delistResource(_, XAResource.TMSUCCESS))
      txnMgr.commit()
      result
    } catch {
      case NonFatal(e) =>
        txnMgr.rollback()
        throw e
    }
  }

}
