package joe.schmoe

import org.scalatest._

import com.hazelcast.Scala._
import java.util.UUID

object TestExecutorService extends ClusterSetup {

  object MemberId extends UserContext.Key[UUID]

  def init = ()

  def destroy = ()

}

class TestExecutorService extends FunSuite with BeforeAndAfterAll {
  import TestExecutorService._

  override def beforeAll() = beforeClass()
  override def afterAll() = afterClass()

  test("user context") {
    hzs.foreach { hz =>
      hz.userCtx(MemberId) = UUID fromString hz.getLocalEndpoint.getUuid
    }
    val es = member.getExecutorService("default")
    val result = es.submit(ToAll) { (hz, callerAddress) =>
      (hz.getLocalEndpoint.getUuid, hz.userCtx(MemberId).toString)
    }
    val resolved = result.mapValues(_.await)
    resolved.foreach {
      case (mbr, (id, uuid)) =>
        assert(id == mbr.getUuid)
        assert(uuid.toString == id)
    }
  }

  test("tasks") {
    val clusterSize = client.getCluster.getMembers.size
    val myMap = client.getMap[Int, String](randName)
    1 to 10000 foreach { i =>
      myMap.set(i, i.toString)
    }
    val exec = client.getExecutorService("executioner")
    val mapName = myMap.getName
    val randomLocal = exec.submit(ToOne) { (hz, callerAddress) =>
      val myMap = hz.getMap[Int, String](mapName)
      myMap.localKeySet.size
    }.await
    val allLocals = exec.submit(ToAll) { (hz, callerAddress) =>
      val myMap = hz.getMap[Int, String](mapName)
      myMap.localKeySet.size
    }.mapValues(_.await)
    assert(allLocals.size == clusterSize)
    assert(allLocals.values.exists(_ == randomLocal))
    assert(allLocals.values.sum == myMap.size)
  }
}
