package joe.schmoe

import com.hazelcast.Scala._
import org.junit._
import org.junit.Assert._
import java.util.UUID
import scala.util.Random
import scala.collection.JavaConverters._

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

  @Test
  def upsert {
    val numbers = getClientCache[UUID, Int]()
    DeltaUpdateTesting.testUpsert(numbers, numbers.get)
//    val key = UUID.randomUUID()
//    numbers.upsert(key, 5)(_ + 1) match {
//      case Update => fail("Should have been Insert")
//      case Insert => assertEquals(5, numbers get key)
//    }
//    numbers.upsert(key, 3)(_ + 9) match {
//      case Update => assertEquals(14, numbers get key)
//      case Insert => fail("Should have been Update")
//    }
//    assertEquals(20, numbers.upsertAndGet(key, 7)(_ + 6))
  }

  @Test
  def update {
    val numbers = getClientCache[UUID, Int]()
    DeltaUpdateTesting.testUpdate(numbers, numbers.get, numbers.put)
//    val key = UUID.randomUUID()
//    assertFalse(numbers.update(key)(_ + 1))
//    numbers.put(key, 3)
//    assertTrue(numbers.update(key)(_ + 4))
//    assertEquals(7, numbers get key)
//    assertEquals(Some(10), numbers.updateAndGet(key)(_ + 3))
  }
}
