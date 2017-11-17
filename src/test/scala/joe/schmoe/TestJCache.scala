package joe.schmoe

import com.hazelcast.Scala.jcache._
import org.junit._
import org.junit.Assert._
import java.util.UUID
import scala.collection.JavaConverters._

object TestJCache extends ClusterSetup {
  //  override def clusterSize = 1
  def init(): Unit = ()
  def destroy(): Unit = ()
}

class TestJCache {
  import TestJCache._

  @Test
  def primitiveTypesUnconfigured() {
    val cache = client.getCache[Int, Long](randName)
    assertNull(cache.get(5))
    cache.putIfAbsent(5, 5L)
    assertEquals(5L, cache.get(5))
  }

  @Test
  def employees() {
    val empCount = 100
    val clientEmployees = client.getCache[UUID, Employee](randName)
    (1 to empCount).foldLeft(member.getCache[UUID, Employee](clientEmployees.getName)) {
      case (mbrEmployees, _) =>
        val emp = Employee.random
        mbrEmployees.put(emp.id, emp)
        mbrEmployees
    }
    assertEquals(empCount, clientEmployees.size)
    clientEmployees.iterator().asScala.foreach { entry =>
      assertEquals(entry.getKey, entry.getValue.id)
    }
  }

  @Test
  def `client names`() {
    val exec = client.getExecutorService("default")
    val m1 = member.getCache(randName)
    val m2 = member.getCache(randName)
    val c1 = client.getCache(randName)
    val c2 = client.getCache(randName)
    val localNames = client.getLocalCacheNames().toSet
    assertTrue(localNames.contains(c1.getName))
    assertTrue(localNames.contains(c2.getName))
    assertFalse(localNames.contains(m1.getName))
    assertFalse(localNames.contains(m2.getName))
    val allNames = client.getClusterCacheNames(exec).await.toSet
    assertTrue(allNames.contains(c1.getName))
    assertTrue(allNames.contains(c2.getName))
    assertTrue(allNames.contains(m1.getName))
    assertTrue(allNames.contains(m2.getName))
  }
  @Test
  def `server names`() {
    val hz = hzs(1)
    val exec = hz.getExecutorService("default")
    val m1 = member.getCache(randName)
    val m2 = member.getCache(randName)
    val localNames = hzs(1).getLocalCacheNames().toSet
    assertFalse(localNames.contains(m1.getName))
    assertFalse(localNames.contains(m2.getName))
    val allNames = hz.getClusterCacheNames(exec).await.toSet
    assertTrue(allNames.contains(m1.getName))
    assertTrue(allNames.contains(m2.getName))
  }
}
