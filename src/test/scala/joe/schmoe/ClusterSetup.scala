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

trait ClusterSetup {
  def randName: String = randomString(50)

  implicit def ec = ExecutionContext.global

  private[this] var _hzs: Vector[HazelcastInstance] = _
  implicit def hzs = _hzs
  private[this] var _client: HazelcastInstance = _
  def client = _client
  def member = hzs(0)

  def clusterSize = 3

  def port = 9991

  final val memberConfig = new Config
  final val clientConfig = new ClientConfig

  def init(): Unit
  def destroy(): Unit

  def beforeClass(): Unit = {
    init()
    val group = UUID.randomUUID.toString
    memberConfig.getGroupConfig.setName(group)
    memberConfig.getNetworkConfig.getJoin.getMulticastConfig.setEnabled(false)
    memberConfig.getNetworkConfig.getJoin.getTcpIpConfig.setEnabled(true).addMember(s"127.0.0.1:$port")
    memberConfig.setGracefulShutdownMaxWait(1.second)
    memberConfig.setPhoneHomeEnabled(false)
    memberConfig.getMapConfig("default")
      .setStatisticsEnabled(false)
      .setMaxSizeConfig(UsedHeapSize(60.gigabytes))
    memberConfig.setShutdownHookEnabled(false)
    _hzs = (0 until clusterSize).map { i =>
      memberConfig.getNetworkConfig.setPort(port + i)
      memberConfig.newInstance
    clientConfig.getGroupConfig.setName(group)
    clientConfig.getNetworkConfig.addAddress(s"127.0.0.1:$port")
    }.toVector
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
