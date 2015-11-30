package com.hazelcast.Scala

import scala.concurrent.Promise
import com.hazelcast.core.ExecutionCallback
import java.util.concurrent.ExecutionException
import com.hazelcast.map.EntryProcessor
import java.util.Map.Entry
import scala.util.control.NonFatal
import com.hazelcast.map.EntryBackupProcessor

private[Scala] sealed abstract class CallbackEntryProcessor[K, V, R] extends EntryProcessor[K, V] {
  final def process(entry: Entry[K, V]): Object =
    try {
      onEntry(entry).asInstanceOf[Object]
    } catch {
      case NonFatal(e) => e
    }
  def onEntry(entry: Entry[K, V]): R

  def newCallback(nullReplacement: R = null.asInstanceOf[R]) = new FutureCallback[R, R](nullReplacement)
  def newCallbackOpt = new FutureCallback[R, Option[R]](None)(Some(_))

}

private[Scala] abstract class CallbackEntryReader[K, V, R] extends CallbackEntryProcessor[K, V, R] {
  def getBackupProcessor = null
  final def onEntry(entry: Entry[K, V]): R = onEntry(entry.key, entry.value)
  def onEntry(key: K, value: V): R
}
private[Scala] abstract class CallbackEntryUpdater[K, V, R] extends CallbackEntryProcessor[K, V, R] {
  def getBackupProcessor = new EntryBackupProcessor[K, V] {
    def processBackup(entry: Entry[K, V]): Unit = onEntry(entry)
  }

}
