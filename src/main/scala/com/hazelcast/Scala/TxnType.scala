package com.hazelcast.Scala

import com.hazelcast.transaction.TransactionOptions

sealed trait TxnType

case object OnePhase extends TxnType
case class TwoPhase(durability: Int = TransactionOptions.getDefault.getDurability) extends TxnType
