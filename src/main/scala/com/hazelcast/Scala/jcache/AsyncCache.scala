package com.hazelcast.Scala.jcache

import scala.concurrent.Future
import com.hazelcast.cache.ICache
import javax.cache.expiry.ExpiryPolicy
import com.hazelcast.Scala.JavaFuture

class AsyncCache[K, V] private[Scala] (private val icache: ICache[K, V]) extends AnyVal {

  def get(key: K)(implicit expiryPolicy: ExpiryPolicy = null): Future[Option[V]] =
    expiryPolicy match {
      case null =>
        icache.getAsync(key).asScalaOpt
      case expiryPolicy =>
        icache.getAsync(key, expiryPolicy).asScalaOpt
    }

  def put(key: K, value: V)(implicit expiryPolicy: ExpiryPolicy = null): Future[Unit] =
    expiryPolicy match {
      case null =>
        icache.putAsync(key, value).asScala(_ => ())
      case expiryPolicy =>
        icache.putAsync(key, value, expiryPolicy).asScala(_ => ())
    }

  def getAndPut(key: K, value: V)(implicit expiryPolicy: ExpiryPolicy = null): Future[Option[V]] =
    expiryPolicy match {
      case null =>
        icache.getAndPutAsync(key, value).asScalaOpt
      case expiryPolicy =>
        icache.getAndPutAsync(key, value, expiryPolicy).asScalaOpt
    }

  def getAndRemove(key: K): Future[Option[V]] = icache.getAndRemoveAsync(key).asScalaOpt

  def getAndReplace(key: K, value: V)(implicit expiryPolicy: ExpiryPolicy = null): Future[Option[V]] =
    expiryPolicy match {
      case null =>
        icache.getAndReplaceAsync(key, value).asScalaOpt
      case expiryPolicy =>
        icache.getAndReplaceAsync(key, value, expiryPolicy).asScalaOpt
    }

  def putIfAbsent(key: K, value: V)(implicit expiryPolicy: ExpiryPolicy = null): Future[Boolean] =
    expiryPolicy match {
      case null =>
        icache.putIfAbsentAsync(key, value).asScala(_.booleanValue)
      case expiryPolicy =>
        icache.putIfAbsentAsync(key, value, expiryPolicy).asScala(_.booleanValue)
    }

  def remove(key: K): Future[Boolean] = icache.removeAsync(key).asScala(_.booleanValue)
  def remove(key: K, expected: V): Future[Boolean] = icache.removeAsync(key, expected).asScala(_.booleanValue)

  def replace(key: K, value: V)(implicit expiryPolicy: ExpiryPolicy = null): Future[Boolean] =
    expiryPolicy match {
      case null =>
        icache.replaceAsync(key, value).asScala(_.booleanValue)
      case expiryPolicy =>
        icache.replaceAsync(key, value, expiryPolicy).asScala(_.booleanValue)
    }
  def replaceIfExpected(key: K, expected: V, newValue: V)(implicit expiryPolicy: ExpiryPolicy = null): Future[Boolean] =
    expiryPolicy match {
      case null =>
        icache.replaceAsync(key, expected, newValue).asScala(_.booleanValue)
      case expiryPolicy =>
        icache.replaceAsync(key, expected, newValue, expiryPolicy).asScala(_.booleanValue)
    }

}
