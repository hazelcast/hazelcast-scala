package joe.schmoe

import org.junit._
import org.junit.Assert._

import com.hazelcast.Scala.actress._
import com.hazelcast.Scala.serialization.SerializerEnum
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput

object TestActress extends ClusterSetup {

  override def clusterSize = 3

  def init = {
    ActressSerializers.register(memberConfig.getSerializationConfig)
  }
  def destroy = ()

  object ActressSerializers extends SerializerEnum(TestKryoSerializers) {
    val JaneFondaSer = new StreamSerializer[JaneFonda] {
      def write(out: ObjectDataOutput, jf: JaneFonda): Unit = {
        out.writeInt(jf.currCounter)
      }
      def read(inp: ObjectDataInput): JaneFonda = {
        new JaneFonda(inp.readInt)
      }
    }
  }

  class JaneFonda(private var counter: Int = 0) {
    def currCounter = counter
    def incrementBy(delta: Int) = counter += delta
  }
}

class TestActress {
  import TestActress._

  @Test
  def foo {
    memberConfig.getMapConfig("Foo").setBackupCount(2)
    val stage: Stage = new Stage("Foo", client)
    val janeFonda = stage.actressOf("fonda/jane", new JaneFonda)
    janeFonda {
      case (_, janeFonda) =>
        janeFonda.incrementBy(3)
        janeFonda.incrementBy(3)
    }.await
    val counterIs6 =
      janeFonda {
        case (_, jf) => jf.currCounter
      }.await
    assertEquals(6, counterIs6)
    val currOwner = client.getPartitionService.getPartition("fonda/jane").getOwner
    val (currHz, twoHzLeft) = hzs.partition(_.getLocalEndpoint.getUuid == currOwner.getUuid)
    currHz.head.shutdown()
    while (!twoHzLeft.head.getPartitionService.isClusterSafe) {
      println("cluster unsafe...")
      Thread sleep 250
    }
    val newCurrOwner = client.getPartitionService.getPartition("fonda/jane").getOwner
    val counterIs10 = janeFonda {
      case (_, jf) =>
        jf.incrementBy(4)
        jf.currCounter
    }
    assertEquals(10, counterIs10.await)
    val (newCurrHz, lastHz) = twoHzLeft.partition(_.getLocalEndpoint.getUuid == newCurrOwner.getUuid)
    newCurrHz.head.shutdown()
    while (!lastHz.head.getPartitionService.isClusterSafe) {
      println("cluster unsafe...")
      Thread sleep 250
    }
    val counterIs5 = janeFonda {
      case (_, jf) =>
        jf.incrementBy(-5)
        jf.currCounter
    }
    assertEquals(5, counterIs5.await)
  }

}
