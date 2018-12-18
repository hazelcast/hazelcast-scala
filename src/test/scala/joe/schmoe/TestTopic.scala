package joe.schmoe

import org.scalatest._
import com.hazelcast.Scala._
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import scala.util.Try

object TestTopic extends ClusterSetup {
  val smallRB = "smallRB"
  val smallRBCapacity = 3
  override def clusterSize = 1
  def init = {
    memberConfig.getRingbufferConfig(smallRB)
      .setCapacity(smallRBCapacity)
    clientConfig.getReliableTopicConfig(smallRB)
      .setReadBatchSize(smallRBCapacity)
  }
  def destroy = ()

}

class TestTopic extends FunSuite with BeforeAndAfterAll {
  import TestTopic._

  override def beforeAll = beforeClass()
  override def afterAll = afterClass()

  test("simple") {
    val messages = Seq(1, 2, 3)

    val cdl = new CountDownLatch(messages.sum)

    val memberFoo = member.getTopic[Int]("foo")
    assert(Try(memberFoo.onSeqMessage()(println(_))).isFailure)

    val registration = memberFoo.onMessage() { msg =>
      val n = msg.get
      for (_ <- 1 to n) cdl.countDown()
    }
    val clientFoo = client.getTopic[Int](memberFoo.getName)
    messages.foreach(clientFoo.publish)
    assert(cdl.await(5, SECONDS))
    registration.cancel()
  }

  test("reliable") {
    val messages = Seq("a", "b", "c")
    val cdl = new CountDownLatch(messages.length)
    val rTopic = client.getReliableTopic[String]("rTopic")
    val reg = rTopic.onSeqMessage() {
      case SeqMessage(seq, value) =>
        assert(messages.length - cdl.getCount === seq)
        assert(messages(seq.toInt) === value)
        cdl.countDown()
    }
    messages.foreach(rTopic.publish)
    assert(cdl.await(5, SECONDS))
    reg.cancel()
  }

  test("stale") {
    val messages = Seq("a", "b", "c", "d", "e")
    val cdl = new CountDownLatch(smallRBCapacity)
    val rTopic = client.getReliableTopic[String](smallRB)
    messages.foreach(rTopic.publish)
    val reg = rTopic.onSeqMessage(startFrom = 0, gapTolerant = true) {
      case SeqMessage(seq, value) =>
        assert(messages.length - cdl.getCount === seq)
        assert(messages(seq.toInt) === value)
        cdl.countDown()
    }
    assert(cdl.await(5, SECONDS))
    reg.cancel()
  }
}
