package joe.schmoe

import com.hazelcast.Scala.jcache._
import org.scalatest._

import java.util.UUID
import scala.collection.JavaConverters._

object TestJCache extends ClusterSetup {
  //  override def clusterSize = 1
  def init(): Unit = ()
  def destroy(): Unit = ()
}

class TestJCache extends FunSuite with BeforeAndAfterAll {
  import TestJCache._

  override def beforeAll() = beforeClass()
  override def afterAll() = afterClass()

  test("primitiveTypesUnconfigured") {
    val cache = client.getCache[Int, Long](randName)
    assert(cache.get(5) === null)
    cache.putIfAbsent(5, 5L)
    assert(cache.get(5) == 5L)
  }

  test("employees") {
    val empCount = 100
    val clientEmployees = client.getCache[UUID, Employee](randName)
    (1 to empCount).foldLeft(member.getCache[UUID, Employee](clientEmployees.getName)) {
      case (mbrEmployees, _) =>
        val emp = Employee.random
        mbrEmployees.put(emp.id, emp)
        mbrEmployees
    }
    assert(clientEmployees.size == empCount)
    clientEmployees.iterator().asScala.foreach { entry =>
      assert(entry.getValue.id == entry.getKey)
    }
  }

  test("client names") {
    val exec = client.getExecutorService("default")
    val m1 = member.getCache(randName)
    val m2 = member.getCache(randName)
    val c1 = client.getCache(randName)
    val c2 = client.getCache(randName)
    val localNames = client.getLocalCacheNames().toSet
    assert(localNames.contains(c1.getName))
    assert(localNames.contains(c2.getName))
    assert(!localNames.contains(m1.getName))
    assert(!localNames.contains(m2.getName))
    val allNames = client.getClusterCacheNames(exec).await.toSet
    assert(allNames.contains(c1.getName))
    assert(allNames.contains(c2.getName))
    assert(allNames.contains(m1.getName))
    assert(allNames.contains(m2.getName))
  }
  test("server names") {
    val hz = hzs(1)
    val exec = hz.getExecutorService("default")
    val m1 = member.getCache(randName)
    val m2 = member.getCache(randName)
    val localNames = hzs(1).getLocalCacheNames().toSet
    assert(!localNames.contains(m1.getName))
    assert(!localNames.contains(m2.getName))
    val allNames = hz.getClusterCacheNames(exec).await.toSet
    assert(allNames.contains(m1.getName))
    assert(allNames.contains(m2.getName))
  }
}
