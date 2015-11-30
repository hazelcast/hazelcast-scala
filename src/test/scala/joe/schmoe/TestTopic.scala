package joe.schmoe

import org.junit._
import org.junit.Assert._
import com.hazelcast.Scala._
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch

object TestTopic extends ClusterSetup {
  override def clusterSize = 1
  def init = ()
  def destroy = ()
}

class TestTopic {
  import TestTopic._

  @Test
  def simple {
    val messages = Seq(1, 2, 3)

    val cdl = new CountDownLatch(messages.sum)

    val clientTopic = client.getTopic[Int]("foo")

    val registration = clientTopic.onMessage { msg =>
      val n = msg.get
      for (_ <- 1 to n) cdl.countDown()
    }
    val memberTopic = hz(0).getTopic[Int](clientTopic.getName)
    messages.foreach(memberTopic.publish)
    assertTrue(cdl.await(5, SECONDS))
    registration.cancel()
  }
}
