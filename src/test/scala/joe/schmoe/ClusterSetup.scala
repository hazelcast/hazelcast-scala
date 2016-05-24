package joe.schmoe

import com.hazelcast.Scala._
import com.hazelcast.Scala.client._
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
import com.hazelcast.instance.HazelcastInstanceFactory
import scala.concurrent.ExecutionContext

trait ClusterSetup {

  implicit def ec = ExecutionContext.global

  private[this] var _hz: Vector[HazelcastInstance] = _
  def hz = _hz
  private[this] var _client: HazelcastInstance = _
  def client = _client

  def clusterSize = 3

  def port = 9991

  final val memberConfig = new Config
  final val clientConfig = new ClientConfig

  def init(): Unit
  def destroy(): Unit

  @BeforeClass
  def beforeClass {
    init()
    val group = UUID.randomUUID.toString
    List(serialization.Defaults, TestSerializers).foreach { serializers =>
      serializers.register(memberConfig.getSerializationConfig)
      serializers.register(clientConfig.getSerializationConfig)
    }
    memberConfig.getGroupConfig.setName(group)
    memberConfig.getNetworkConfig.setPort(port)
    memberConfig.setGracefulShutdownMaxWait(1.second)
    memberConfig.setPhoneHomeEnabled(false)
    memberConfig.getMapConfig("default")
//      .setBackupCount(0)
      .setStatisticsEnabled(false)
      .setMaxSizeConfig(UsedHeapSize(60.gigabytes))
    memberConfig.setShutdownHookEnabled(false)
    _hz = (1 to clusterSize).par.map(_ => memberConfig.newInstance).seq.toVector
    clientConfig.getGroupConfig.setName(group)
    clientConfig.getNetworkConfig.addAddress(s"localhost:$port")
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
    import jcache._
    client.getCache[K, V](name)
  }

  def getMemberMap[K, V](name: String = contextName): IMap[K, V] = hz(0).getMap[K, V](name)
  def getMemberCache[K: ClassTag, V: ClassTag](name: String = contextName): ICache[K, V] = {
    import jcache._
    hz(0).getCache[K, V](name)
  }

  def timed[T](warmups: Int = 0, unit: TimeUnit = MILLISECONDS)(thunk: => T): (T, Long) = {
    (0 until warmups).foreach(_ => thunk)
    val start = System.nanoTime
    thunk -> unit.convert(System.nanoTime - start, NANOSECONDS)
  }

}
