package com.hazelcast.Scala

import com.hazelcast.client.config.ClientConfig

import language.implicitConversions

package object client {
  @inline implicit def clientConf2scala(conf: ClientConfig) = new HzClientConfig(conf)
  @inline implicit def clientConf2props(conf: ClientConfig) = new HzClientProperties(conf)
}
