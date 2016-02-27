package com.hazelcast.Scala

import com.hazelcast.cache.ICache
import com.hazelcast.core.HazelcastInstance
import javax.cache.spi.CachingProvider
import language.implicitConversions
import java.lang.reflect.Method
import scala.util.Try

package object jcache {

  private[this] val HazelcastClientCachingProvider_createCachingProvider: Try[Method] = Try {
    Class.forName("com.hazelcast.client.cache.impl.HazelcastClientCachingProvider")
      .getDeclaredMethod("createCachingProvider", classOf[HazelcastInstance])
  }

  private[Scala] def createClientCachingProvider(hz: HazelcastInstance): Try[CachingProvider] =
    HazelcastClientCachingProvider_createCachingProvider.map(_.invoke(null, hz).asInstanceOf[CachingProvider])

  implicit def inst2jcache(hz: HazelcastInstance): JCacheHazelcastInstance = new JCacheHazelcastInstance(hz)
  implicit def icache2scala[K, V](icache: ICache[K, V]) = new HzCache[K, V](icache)
}
