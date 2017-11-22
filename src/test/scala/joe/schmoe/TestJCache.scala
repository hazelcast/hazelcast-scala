package joe.schmoe

import com.hazelcast.Scala._
import com.hazelcast.Scala.jcache._
import org.junit._
import org.junit.Assert._
import java.util.UUID
import scala.util.Random
import scala.collection.JavaConverters._
import com.hazelcast.core.DistributedObject

object TestJCache extends ClusterSetup {
  //  override def clusterSize = 1
  def init = ()
  def destroy = ()
}

class TestJCache {
  import TestJCache._

  @Test
  def primitiveTypesUnconfigured {
    val cache = getClientCache[Int, Long]()
    assertNull(cache.get(5))
    cache.putIfAbsent(5, 5L)
    assertEquals(5L, cache.get(5))
  }

  @Test
  def employees {
    val empCount = 100
    val employees = getClientCache[UUID, Employee]()
    (1 to empCount).foldLeft(getMemberCache[UUID, Employee](employees.getName)) {
      case (employees, _) =>
        val emp = Employee.random
        employees.put(emp.id, emp)
        employees
    }
    assertEquals(empCount, employees.size)
    employees.iterator().asScala.foreach { entry =>
      assertEquals(entry.getKey, entry.getValue.id)
    }
  }

  @Test @Ignore
  def upsert {
    val numbers = getClientCache[UUID, Int]()
//    DeltaUpdateTesting.testUpsert(numbers, key => Option(numbers.get(key)), key => numbers.remove(key))
  }

  @Test @Ignore
  def update {
    val numbers = getClientCache[UUID, Int]()
//    DeltaUpdateTesting.testUpdate(numbers, key => Option(numbers.get(key)), numbers.put, key => numbers.remove(key))
  }

  @Test
  def `client names` {
    val exec = client.getExecutorService("default")
    val m1 = getMemberCache()
    val m2 = getMemberCache()
    val c1 = getClientCache()
    val c2 = getClientCache()
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
  def `server names` {
    val hz = hzs(1)
    val exec = hz.getExecutorService("default")
    val m1 = getMemberCache()
    val m2 = getMemberCache()
    val localNames = hzs(1).getLocalCacheNames().toSet
    assertFalse(localNames.contains(m1.getName))
    assertFalse(localNames.contains(m2.getName))
    val allNames = hz.getClusterCacheNames(exec).await.toSet
    assertTrue(allNames.contains(m1.getName))
    assertTrue(allNames.contains(m2.getName))
  }
}
