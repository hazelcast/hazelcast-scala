package joe.schmoe

import java.util.UUID

import scala.concurrent.duration._

import org.scalatest._

import com.hazelcast.Scala._

object TestTransaction extends ClusterSetup {
  override val clusterSize = 3
  def init: Unit = {
    TestKryoSerializers.register(memberConfig.getSerializationConfig)
    TestKryoSerializers.register(clientConfig.getSerializationConfig)
    memberConfig.getSerializationConfig.setAllowUnsafe(true)
    clientConfig.getSerializationConfig.setAllowUnsafe(true)
  }
  def destroy = ()
  case class MyNumber(num: Int)
}

class TestTransaction extends FunSuite with CleanUp with BeforeAndAfterAll with BeforeAndAfter {

  import TestTransaction._

  def hzs = TestTransaction.hzs

  override def beforeAll = beforeClass()
  override def afterAll = afterClass()

  after {
    cleanup
  }

  test("successful transaction") {
    val queueName = UUID.randomUUID.toString
    val item = UUID.randomUUID
    assert(client.getQueue[UUID](queueName).offer(item, 10.seconds))
    client.transaction(timeout = 20.seconds) { context =>
      val queue = context.getQueue[UUID](queueName)
      assert(queue.peek(1.second) != null)
      val uuid = queue.poll(1.second)
      context.getMap("uuids").set(uuid, uuid.toString)
    }
    assert(client.getQueue[UUID](queueName).poll() == null)
    val uuidStr = client.getMap[UUID, String]("uuids").get(item)
    assert(uuidStr == item.toString)
  }

  test("failed transaction") {
    val queueName = UUID.randomUUID.toString
    val item = UUID.randomUUID
    assert(client.getQueue[UUID](queueName).offer(item, 10.seconds))
    try client.transaction(timeout = 20.seconds) { context =>
      val queue = context.getQueue[UUID](queueName)
      assert(queue.peek(1.second) == item)
      val uuid = queue.poll(10.seconds)
      assert(uuid == item)
      assert(queue.peek(10.seconds) == null)
      throw new IllegalStateException("oh noes...")
    } catch {
      case _: IllegalStateException => // Ignore
    }
    assert(client.getQueue[UUID](queueName).peek() == item)
  }

  test("beginTransaction/commitTransaction") {
    val queueName = UUID.randomUUID.toString
    val item = UUID.randomUUID
    assert(client.getQueue[UUID](queueName).offer(item, 10.seconds))
    client.beginTransaction(timeout = 20.seconds) { context =>
      val queue = context.getQueue[UUID](queueName)
      assert(queue.peek(1.second) != null)
      val uuid = queue.poll(1.second)
      context.getMap("uuids").set(uuid, uuid.toString)
      context.commitTransaction()
    }
    assert(client.getQueue[UUID](queueName).poll() == null)
    val uuidStr = client.getMap[UUID, String]("uuids").get(item)
    assert(uuidStr == item.toString)
  }

  test("beginTransaction/rollbackTransaction") {
    val queueName = UUID.randomUUID.toString
    val item = UUID.randomUUID
    assert(client.getQueue[UUID](queueName).offer(item, 10.seconds))
    client.beginTransaction(timeout = 20.seconds) { context =>
      val queue = context.getQueue[UUID](queueName)
      assert(queue.peek(1.second) == item)
      val uuid = queue.poll(10.seconds)
      assert(uuid == item)
      assert(queue.peek(10.seconds) == null)
      context.rollbackTransaction()
    }
    assert(client.getQueue[UUID](queueName).peek() == item)
  }

}
