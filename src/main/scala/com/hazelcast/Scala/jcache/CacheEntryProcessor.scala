package com.hazelcast.Scala.jcache

import com.hazelcast.cache.BackupAwareEntryProcessor

private[jcache] abstract class CacheEntryProcessor[K, V, R] extends BackupAwareEntryProcessor[K, V, R] with java.io.Serializable {
  def createBackupEntryProcessor() = this
}
