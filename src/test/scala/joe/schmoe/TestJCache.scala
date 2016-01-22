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

}
