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
    val employees = getClientCache[UUID, Employee]()
    (1 to 100) foreach { i =>
      val emp = Employee.random
      employees.async.put(emp.id, emp)
    }
    employees.iterator().asScala.foreach { entry =>
      assertEquals(entry.getKey, entry.getValue.id)
    }
  }

}
