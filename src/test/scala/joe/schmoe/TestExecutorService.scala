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
    hz.foreach { hz =>
      hz.userCtx(MemberId) = UUID fromString hz.getLocalEndpoint.getUuid
    }
    val es = hz(0).getExecutorService("default")
    val result = es.submitInstanceAware(ToAll) { hz =>
      hz.getLocalEndpoint.getUuid -> hz.userCtx(MemberId)
    }
    val resolved = result.mapValues(_.await)
    resolved.foreach {
      case (mbr, (id, uuid)) =>
        assertEquals(mbr.getUuid, id)
        assertEquals(id, uuid.toString)
    }
  }
}
