package joe.schmoe

import java.util.UUID

import com.hazelcast.Scala._
import com.hazelcast.Scala.client._
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.config.Config
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.instance.HazelcastInstanceFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

trait ClusterSetup {
  def randName: String = randomString(50)

  implicit def ec = ExecutionContext.global

  private[this] var _hzs: Vector[HazelcastInstance] = _
  implicit def hzs = _hzs
  private[this] var _client: HazelcastInstance = _
  def client = _client
  def member = hzs(0)

  def clusterSize = 3

  final val port = 49152 + Random.nextInt(9999)

  final val memberConfig = new Config
  final val clientConfig = new ClientConfig

  def init(): Unit
  def destroy(): Unit

  def beforeClass(): Unit = {
    init()
    val group = UUID.randomUUID.toString
    val passw = UUID.randomUUID.toString
    memberConfig.getNetworkConfig.getJoin.getMulticastConfig.setEnabled(false)
    memberConfig.getNetworkConfig.getJoin.getTcpIpConfig.setEnabled(true).addMember(s"127.0.0.1:$port")
    memberConfig.getGroupConfig.setName(group).setPassword(passw)
    memberConfig.setGracefulShutdownMaxWait(1.second)
    memberConfig.setPhoneHomeEnabled(false)
    memberConfig.getMapConfig("default")
      .setStatisticsEnabled(false)
      .setMaxSizeConfig(UsedHeapSize(60.gigabytes))
    memberConfig.setShutdownHookEnabled(false)
    _hzs = (0 until clusterSize).map { i =>
      memberConfig.getNetworkConfig.setPort(port + i)
      memberConfig.newInstance
    }.toVector
    clientConfig.getGroupConfig.setName(group).setPassword(passw)
    (0 until clusterSize).foldLeft(clientConfig.getNetworkConfig) {
      case (netConf, i) => netConf.addAddress(s"127.0.0.1:${port+i}")
    }
    clientConfig.getNetworkConfig.setConnectionAttemptLimit(100)
    _client = clientConfig.newClient()
  }

  def afterClass() {
    destroy()
    _client.shutdown()
    HazelcastInstanceFactory.terminateAll()
  }

  def timed[T](warmups: Int = 0, unit: TimeUnit = MILLISECONDS)(thunk: => T): (T, Long) = {
    (0 until warmups).foreach(_ => thunk)
    val start = System.nanoTime
    thunk -> unit.convert(System.nanoTime - start, NANOSECONDS)
  }
}
