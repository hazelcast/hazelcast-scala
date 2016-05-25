package joe.schmoe

import org.junit._
import org.junit.Assert._
import com.hazelcast.Scala._
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._

object TestExecutorService extends ClusterSetup {

  object MemberId extends UserContext.Key[UUID]

  def init = ()
  def destroy = ()
}

class TestExecutorService {
  import TestExecutorService._

  @Test
  def `user context` {
    hzs.foreach { hz =>
      hz.userCtx(MemberId) = UUID fromString hz.getLocalEndpoint.getUuid
    }
    val es = hzs(0).getExecutorService("default")
    val result = es.submit(ToAll) { hz =>
      hz.getLocalEndpoint.getUuid -> hz.userCtx(MemberId)
    }
    val resolved = result.mapValues(_.await)
    resolved.foreach {
      case (mbr, (id, uuid)) =>
        assertEquals(mbr.getUuid, id)
        assertEquals(id, uuid.toString)
    }
  }

  @Test
  def `tasks` {
    val clusterSize = client.getCluster.getMembers.size
    val myMap = getClientMap[Int, String]()
    1 to 10000 foreach { i =>
      myMap.set(i, i.toString)
    }
    val exec = client.getExecutorService("executioner")
    val mapName = myMap.getName
    val randomLocal = exec.submit(ToOne) { hz =>
      val myMap = hz.getMap[Int, String](mapName)
      myMap.localKeySet.size
    }.await
    val allLocals = exec.submit(ToAll) { hz =>
      val myMap = hz.getMap[Int, String](mapName)
      myMap.localKeySet.size
    }.mapValues(_.await)
    assertEquals(clusterSize, allLocals.size)
    assertTrue(allLocals.values.exists(_ == randomLocal))
    assertEquals(myMap.size, allLocals.values.sum)
  }
}
