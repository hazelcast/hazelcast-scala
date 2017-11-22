package com.hazelcast.Scala

import com.hazelcast.core.IMap

object HackIntegrityTesting {
  def verifyFastAccess(imap: IMap[_, _]): Boolean = {
    HzMap.mapServiceContext(imap).get != null
  }
}
