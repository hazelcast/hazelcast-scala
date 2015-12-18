package joe.schmoe

import com.hazelcast.Scala._
import org.junit.BeforeClass
import com.hazelcast.core.HazelcastInstance
import org.junit.AfterClass
import java.util.UUID
import com.hazelcast.core.Hazelcast
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.client.HazelcastClient
import com.hazelcast.config.Config
import com.hazelcast.core.IMap
import com.hazelcast.cache.ICache
import scala.reflect.ClassTag
import scala.util.Random
import scala.concurrent.duration._
import com.hazelcast.Scala.serialization.DefaultSerializers
import com.hazelcast.instance.HazelcastInstanceFactory

trait ClusterSetup {
  private[this] var _hz: Vector[HazelcastInstance] = _
  def hz = _hz
  private[this] var _client: HazelcastInstance = _
  def client = _client

  def clusterSize = 3

  final val memberConfig = new Config
  final val clientConfig = new ClientConfig

  def init(): Unit
  def destroy(): Unit

  @BeforeClass
  def beforeClass {
    init()
    val group = UUID.randomUUID.toString
    DefaultSerializers.register(memberConfig.getSerializationConfig)
    DefaultSerializers.register(clientConfig.getSerializationConfig)
    memberConfig.getGroupConfig.setName(group)
    //    memberConfig.setClusterShutdownTimeout(2.seconds)
    memberConfig.getMapConfig("default").setBackupCount(0).setStatisticsEnabled(false)
    memberConfig.setShutdownHookEnabled(false)
    _hz = (1 to clusterSize).par.map(_ => memberConfig.newInstance).seq.toVector
    clientConfig.getGroupConfig.setName(group)
    _client = clientConfig.newClient()
  }

  @AfterClass
  def afterClass {
    destroy()
    _client.shutdown
    HazelcastInstanceFactory.terminateAll()
  }

  private def contextName: String = {
    val st = Thread.currentThread().getStackTrace.drop(1)
    st.dropWhile(_.getClassName.contains("$")).head.getMethodName + "-" + UUID.randomUUID.toString.replace("-", "")
  }

  def getClientMap[K, V](name: String = contextName): IMap[K, V] = client.getMap[K, V](name)
  def getClientCache[K: ClassTag, V: ClassTag](name: String = contextName): ICache[K, V] = {
    //    getMemberCache[K, V](name) // Init, if missing
    client.getCache[K, V](name)
  }

  def getMemberMap[K, V](name: String = contextName): IMap[K, V] = hz(0).getMap[K, V](name)
  def getMemberCache[K: ClassTag, V: ClassTag](name: String = contextName): ICache[K, V] = hz(0).getCache[K, V](name)

  def timed[T](unit: TimeUnit = MILLISECONDS)(thunk: => T): (T, Long) = {
    val start = System.nanoTime
    thunk -> unit.convert(System.nanoTime - start, NANOSECONDS)
  }

}
