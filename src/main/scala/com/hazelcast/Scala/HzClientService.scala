package com.hazelcast.Scala

import com.hazelcast.client.ClientService
import scala.concurrent.ExecutionContext

class HzClientService(private val service: ClientService) extends AnyVal {
  def onClient(runOn: ExecutionContext = null)(listener: PartialFunction[ClientEvent, Unit]): ListenerRegistration = {
    val regId = service addClientListener EventSubscription.asClientListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel(): Boolean = service removeClientListener regId
    }
  }

}
