package joe.schmoe

import org.junit._
import org.junit.Assert._
import com.hazelcast.Scala._
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import com.hazelcast.core.Message
import scala.util.Try
import com.hazelcast.config.RingbufferConfig

object TestTopic extends ClusterSetup {
  val smallRB = "smallRB"
  val smallRBCapacity = 3
  override def clusterSize = 1
  def init = {
    val rbConf = new RingbufferConfig(smallRB)
    rbConf.setCapacity(smallRBCapacity)
    memberConfig.addRingBufferConfig(rbConf)
  }
  def destroy = ()
}

class TestTopic {
  import TestTopic._

  @Test
  def simple {
    val messages = Seq(1, 2, 3)

    val cdl = new CountDownLatch(messages.sum)

    val memberFoo = hzs(0).getTopic[Int]("foo")
    assertTrue(Try(memberFoo.onSeqMessage()(println(_))).isFailure)

    val registration = memberFoo.onMessage() { msg =>
      val n = msg.get
      for (_ <- 1 to n) cdl.countDown()
    }
    val clientFoo = client.getTopic[Int](memberFoo.getName)
    messages.foreach(clientFoo.publish)
    assertTrue(cdl.await(5, SECONDS))
    registration.cancel()
  }
  @Test
  def reliable {
    val messages = Seq("a", "b", "c")
    val cdl = new CountDownLatch(messages.length)
    val rTopic = client.getReliableTopic[String]("rTopic")
    val reg = rTopic.onSeqMessage() {
      case (seq, msg) =>
        assertEquals(messages.length - cdl.getCount: Long, seq)
        assertEquals(messages(seq.toInt), msg.get)
        cdl.countDown()
    }
    messages.foreach(rTopic.publish)
    assertTrue(cdl.await(5, SECONDS))
    reg.cancel()
  }
  @Test @Ignore // FIXME: Un-ignore when fixed: https://github.com/hazelcast/hazelcast/issues/7317
  def stale {
    val messages = Seq("a", "b", "c", "d", "e")
    val cdl = new CountDownLatch(smallRBCapacity)
    val rTopic = client.getReliableTopic[String](smallRB)
    messages.foreach(rTopic.publish)
    val reg = rTopic.onSeqMessage(startFrom = 0, gapTolerant = true) {
      case (seq, msg) =>
        assertEquals(messages.length - cdl.getCount: Long, seq)
        assertEquals(messages(seq.toInt), msg.get)
        cdl.countDown()
    }
    assertTrue(cdl.await(5, SECONDS))
    reg.cancel()
  }
}
