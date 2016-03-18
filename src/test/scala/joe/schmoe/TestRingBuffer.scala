package joe.schmoe

import org.junit._
import org.junit.Assert._
import com.hazelcast.Scala._
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.Future

object TestRingBuffer extends ClusterSetup {
  override def clusterSize = 1
  def init = ()
  def destroy = ()
}

class TestRingBuffer {
  import TestRingBuffer._

  @Test
  def async {
    val isNumber = (s: String) => s.length > 0 && s.forall(c => c >= '0' && c <= '9')
    val rb = client.getRingbuffer[String](UUID.randomUUID.toString)
    var itemsRead = rb.async.readBatch(startFrom = 0, 0)(_ => Unit).await
    assertEquals(0, itemsRead)
    val values = Seq("a", "123", "b", "456", "c", "789", "d", "987", "e")
    val lastSeqAdded = rb.async.addAll(values).await.get
    assertEquals(values.length - 1L, lastSeqAdded)
    assertEquals(values.last, rb.readOne(lastSeqAdded))
    var numbers = Vector.empty[Int]
    itemsRead = rb.async.readBatch(startFrom = 2, 2 to 1000) {
      case str if isNumber(str) => numbers :+= str.toInt
    }.await
    assertEquals(Vector(456, 789, 987), numbers)
    assertEquals(values.length-2, itemsRead)
    itemsRead = rb.async.readBatch(startFrom = 2 + itemsRead, 0)(println(_)).await
    assertEquals(0, itemsRead)
  }

  @Test
  def `verify batch` {
    val cdl = new CountDownLatch(1)
    val rb = client.getRingbuffer[String](UUID.randomUUID.toString)
    val readCount = Future {
      rb.async.readBatch(0)(_ => Unit)
    }.flatMap(identity)
    val values = Seq("a", "b", "c")
    rb.async.addAll(values) andThen { _ =>
      assertEquals(values.size, readCount.await)
      cdl.countDown()
    }
    assertTrue(cdl.await(5, SECONDS))
  }
}
