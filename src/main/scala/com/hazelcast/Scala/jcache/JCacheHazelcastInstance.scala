package com.hazelcast.Scala.jcache

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.util.Try

import com.hazelcast.Scala._
import com.hazelcast.cache.ICache
import com.hazelcast.cache.impl.HazelcastServerCachingProvider
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider
import com.hazelcast.core._
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.instance.GroupProperty
import com.hazelcast.instance.HazelcastProperty

import javax.cache.CacheManager

private object JCacheHazelcastInstance {
  val CacheManagers = new TrieMap[HazelcastInstance, CacheManager]
  val PrimitiveWrappers: Map[Class[_], Class[_]] = Map(
    classOf[Boolean] -> classOf[java.lang.Boolean],
    classOf[Byte] -> classOf[java.lang.Byte],
    classOf[Short] -> classOf[java.lang.Short],
    classOf[Char] -> classOf[java.lang.Character],
    classOf[Float] -> classOf[java.lang.Float],
    classOf[Int] -> classOf[java.lang.Integer],
    classOf[Double] -> classOf[java.lang.Double],
    classOf[Long] -> classOf[java.lang.Long])
}

class JCacheHazelcastInstance(private val hz: HazelcastInstance) extends AnyVal {
  import JCacheHazelcastInstance._

  private def getProperty(prop: HazelcastProperty): Option[String] = {
    val name = prop.getName
    val conf = Try(hz.getConfig) orElse Try(hz.getClass.getMethod("getClientConfig")).map(_.invoke(hz))
    val value = conf.map { conf =>
      val getProperty = conf.getClass.getMethod("getProperty", classOf[String])
      Option(getProperty.invoke(conf, name)).map(_.toString)
    }
    value getOrElse Option(System.getProperty(name))
  }

  private def getObjectType[T: ClassTag]: Class[T] = classTag[T].runtimeClass match {
    case cls if cls.isPrimitive => PrimitiveWrappers(cls).asInstanceOf[Class[T]]
    case cls => cls.asInstanceOf[Class[T]]
  }

  private def getCacheProvider[K, V](cacheName: String, entryTypes: Option[(Class[K], Class[V])]) = {
      def setClassType(classType: Class[_], getType: () => String, setType: String => Unit) {
        val typeName = classType.getName
        getType() match {
          case null =>
            setType(typeName)
          case configured if configured != typeName =>
            sys.error(s"""Type $typeName, for cache "$cacheName", does not match configured type $configured""")
          case _ => // Already set and matching
        }
      }
    val isClient = getProperty(GroupProperty.JCACHE_PROVIDER_TYPE) match {
      case Some("client") => true
      case Some("server") => false
      case Some(other) => sys.error(s"Unknown provider type: $other")
      case None => hz.isClient
    }
    if (isClient) {
      HazelcastClientCachingProvider.createCachingProvider(hz)
    } else {
      entryTypes.foreach {
        case (keyType, valueType) =>
          val conf = hz.getConfig.getCacheConfig(cacheName)
          setClassType(keyType, conf.getKeyType, conf.setKeyType)
          setClassType(valueType, conf.getValueType, conf.setValueType)
      }
      HazelcastServerCachingProvider.createCachingProvider(hz)
    }
  }

  def getCache[K: ClassTag, V: ClassTag](name: String, typesafe: Boolean = true): ICache[K, V] = {
    val entryType = if (typesafe) {
      Some(getObjectType[K] -> getObjectType[V])
    } else None
    val mgr = CacheManagers.get(hz) getOrElse {
      val mgr = getCacheProvider(name, entryType).getCacheManager
      CacheManagers.putIfAbsent(hz, mgr) getOrElse {
        hz.onLifecycleStateChange() {
          case LifecycleState.SHUTDOWN => CacheManagers.remove(hz)
        }
        mgr
      }
    }
    val cache = entryType.map {
      case (keyType, valueType) => mgr.getCache[K, V](name, keyType, valueType)
    }.getOrElse(mgr.getCache[K, V](name)) match {
      case null =>
        val cc = new javax.cache.configuration.Configuration[K, V] {
          def getKeyType() = entryType.map(_._1) getOrElse classOf[Object].asInstanceOf[Class[K]]
          def getValueType() = entryType.map(_._2) getOrElse classOf[Object].asInstanceOf[Class[V]]
          def isStoreByValue() = true
        }
        mgr.createCache[K, V, cc.type](name, cc)
      case cache => cache
    }
    cache.unwrap(classOf[ICache[K, V]])
  }
}
