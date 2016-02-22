package com.hazelcast.Scala

import com.hazelcast.core.HazelcastInstance

package object xa {
  implicit def inst2xa(hz: HazelcastInstance): XAHazelcastInstance = new XAHazelcastInstance(hz)
}
