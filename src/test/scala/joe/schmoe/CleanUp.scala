package joe.schmoe

import org.junit.After
import com.hazelcast.core.HazelcastInstance
import collection.JavaConversions._

trait CleanUp {

  def hzs: Vector[HazelcastInstance]

  @After
  def cleanup {
    hzs.foreach { hz =>
      hz.getDistributedObjects.foreach(_.destroy)
    }
  }
}
