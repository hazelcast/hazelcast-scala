package com.hazelcast.Scala

import com.hazelcast.core.Client

sealed trait ClientEvent {
  def client: Client
}

case class ClientConnected(client: Client) extends ClientEvent
case class ClientDisconnected(client: Client) extends ClientEvent
