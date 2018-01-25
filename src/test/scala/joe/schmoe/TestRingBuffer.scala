package joe.schmoe

import org.scalatest._
import com.hazelcast.Scala._
import java.util.UUID
import scala.concurrent.Future

object TestRingBuffer extends ClusterSetup {
  override def clusterSize = 1
  def init = ()
  def destroy = ()
}

class TestRingBuffer extends FunSuite with BeforeAndAfterAll {
  import TestRingBuffer._

  override def beforeAll = beforeClass()
  override def afterAll = afterClass()

  test("read nothing from empty buffer and no minimum") {
    val rb = client.getRingbuffer[String](UUID.randomUUID.toString)
    val itemsRead = rb.async.readBatch(startFrom = 0, minItems = 0)(_ => Unit).await
    assert(itemsRead == 0)
  }
  test("sequence number alignment") {
    val isNumber = (s: String) => s.length > 0 && s.forall(c => c >= '0' && c <= '9')
    val rb = client.getRingbuffer[String](UUID.randomUUID.toString)
    val values = Seq("a", "123", "b", "456", "c", "789", "d", "987", "e")
    val Some(lastSeqAdded) = rb.async.addAll(values).await
    assert(lastSeqAdded == values.length - 1L)
    assert(rb.readOne(lastSeqAdded) == values.last)
    @volatile var numbers = Vector.empty[Int]
    val itemsRead = rb.async.readBatch(startFrom = 2, 2 to 1000) {
      case str if isNumber(str) => numbers :+= str.toInt
    }.await
    assert(numbers == Vector(456, 789, 987))
    assert(itemsRead == values.length - 2)
    val itemsReadAgain = rb.async.readBatch(startFrom = 2 + itemsRead, 0)(println(_)).await
    assert(itemsReadAgain == 0)
  }

  test("verify batch") {
    val rb = client.getRingbuffer[String](UUID.randomUUID.toString)
    //    val cdl = new CountDownLatch(1)
    val readCount = Future {
      rb.async.readBatch(0)(_ => Unit)
    }.flatMap(identity)
    val values = Seq("a", "b", "c")
    val Some(lastSeq) = rb.async.addAll(values).await
    assert(values.size === readCount.await)
    assert(lastSeq == values.size - 1)
  }
}
