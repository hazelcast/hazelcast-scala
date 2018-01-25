package com.hazelcast.Scala.jcache

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.{ ClassTag, classTag }
import scala.util.Try

import com.hazelcast.Scala._
import com.hazelcast.cache.ICache
import com.hazelcast.cache.impl.HazelcastServerCachingProvider
import com.hazelcast.core.{ HazelcastInstance, IExecutorService }
import com.hazelcast.core.LifecycleEvent.LifecycleState
import com.hazelcast.spi.properties.{ GroupProperty, HazelcastProperty }

import javax.cache.CacheManager

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
object JCacheHazelcastInstance {
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

  def getLocalCacheNames(): Iterable[String] = getCacheManager().getCacheNames.asScala

  def getClusterCacheNames(exec: IExecutorService)(
      implicit ec: ExecutionContext): Future[Iterable[String]] = {
    val byMember = exec.submit(ToAll) { hz =>
      hz.getDistributedObjects.iterator.asScala
        .filter(_.isInstanceOf[javax.cache.Cache[_, _]])
        .map(_.getName).toVector
    }
    Future.sequence(byMember.values).map(_.flatten.toSet)
  }

  private def getCacheManager[K, V](): CacheManager =
    CacheManagers.get(hz) getOrElse {
      val mgr = {
        val isClient = getProperty(GroupProperty.JCACHE_PROVIDER_TYPE) match {
          case Some("client") => true
          case Some("server") => false
          case Some(other) => sys.error(s"Unknown provider type: $other")
          case None => hz.isClient
        }
        val provider =
          if (isClient) {
            createClientCachingProvider(hz).get
          } else {
            HazelcastServerCachingProvider.createCachingProvider(hz)
          }
        provider.getCacheManager
      }
      CacheManagers.putIfAbsent(hz, mgr) getOrElse {
        var shutdownReg: ListenerRegistration = null
        shutdownReg = hz.onLifecycleStateChange() {
          case LifecycleState.SHUTDOWN =>
            Try(CacheManagers.remove(hz).foreach(_.close))
            val reg = shutdownReg
            if (reg != null) {
              shutdownReg = null
              reg.cancel()
            }
        }
        mgr
      }
    }

  def getCache[K: ClassTag, V: ClassTag](name: String, typesafe: Boolean = true): ICache[K, V] = {
    val entryTypes = if (typesafe) {
      Some(getObjectType[K] -> getObjectType[V])
    } else None
    val mgr = getCacheManager()
    val cache = entryTypes.map {
      case (keyType, valueType) => mgr.getCache[K, V](name, keyType, valueType)
    }.getOrElse(mgr.getCache[K, V](name)) match {
      case null =>
        val cc = new javax.cache.configuration.Configuration[K, V] {
          def getKeyType() = entryTypes.map(_._1) getOrElse classOf[Object].asInstanceOf[Class[K]]
          def getValueType() = entryTypes.map(_._2) getOrElse classOf[Object].asInstanceOf[Class[V]]
          def isStoreByValue() = true
        }
        mgr.createCache[K, V, cc.type](name, cc)
      case cache => cache
    }
    cache.unwrap(classOf[ICache[K, V]])
  }
}
