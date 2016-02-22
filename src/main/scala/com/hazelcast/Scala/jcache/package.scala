package com.hazelcast.Scala

import com.hazelcast.cache.ICache

import com.hazelcast.core.HazelcastInstance

import language.implicitConversions

package object jcache {
  implicit def inst2jcache(hz: HazelcastInstance): JCacheHazelcastInstance = new JCacheHazelcastInstance(hz)
  implicit def icache2scala[K, V](icache: ICache[K, V]) = new HzCache[K, V](icache)
}
