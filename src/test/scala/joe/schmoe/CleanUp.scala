package joe.schmoe

import org.junit.After
import com.hazelcast.core.HazelcastInstance
import collection.JavaConverters._

trait CleanUp {

  def hzs: Vector[HazelcastInstance]

  @After
  def cleanup() {
    hzs.foreach { hz =>
      hz.getDistributedObjects.asScala.foreach(_.destroy)
    }
  }
}
