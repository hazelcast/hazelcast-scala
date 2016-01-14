package com.hazelcast.Scala

import com.hazelcast.config.Config
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import com.hazelcast.internal.monitors.HealthMonitorLevel
import com.hazelcast.logging.LoggerFactory
import com.hazelcast.core.PartitioningStrategy
import com.hazelcast.memory.MemorySize
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.internal.metrics.ProbeLevel
import com.hazelcast.query.impl.predicates.QueryOptimizerFactory

class HzClientProperties(private val conf: ClientConfig) extends AnyVal {
  import com.hazelcast.client.config.ClientProperty._

  /** @see com.hazelcast.client.config.ClientProperties.EVENT_QUEUE_CAPACITY */
  def setEventQueueCapacity(cap: Int): ClientConfig =
    conf.setProperty(EVENT_QUEUE_CAPACITY, cap.toString)
  /** @see com.hazelcast.client.config.ClientProperties.EVENT_THREAD_COUNT */
  def setEventThreadCount(threads: Int): ClientConfig =
    conf.setProperty(EVENT_THREAD_COUNT, threads.toString)
  /** @see com.hazelcast.client.config.ClientProperties.HEARTBEAT_INTERVAL */
  def setHeartbeatInterval(interval: FiniteDuration): ClientConfig =
    conf.setProperty(HEARTBEAT_INTERVAL, interval.toMillis.toString)
  /** @see com.hazelcast.client.config.ClientProperties.HEARTBEAT_TIMEOUT */
  def setHeartbeatTimeout(timeout: FiniteDuration): ClientConfig =
    conf.setProperty(HEARTBEAT_TIMEOUT, timeout.toMillis.toString)
  /** @see com.hazelcast.client.config.ClientProperties.INVOCATION_TIMEOUT_SECONDS */
  def setInvocationTimeout(timeout: FiniteDuration): ClientConfig =
    conf.setProperty(INVOCATION_TIMEOUT_SECONDS, timeout.toSeconds.toString)
  /** @see com.hazelcast.client.config.ClientProperties.SHUFFLE_MEMBER_LIST */
  def setShuffleMemberList(shuffle: Boolean): ClientConfig =
    conf.setProperty(SHUFFLE_MEMBER_LIST, shuffle.toString)
  /** @see com.hazelcast.client.config.ClientProperties.MAX_CONCURRENT_INVOCATIONS */
  def setMaxConcurrentInvocations(max: Int): ClientConfig =
    conf.setProperty(MAX_CONCURRENT_INVOCATIONS, max.toString)
}

class HzMemberProperties(private val conf: Config) extends AnyVal {
  import com.hazelcast.instance.GroupProperty._

  /** @see com.hazelcast.instance.GroupProperty.SYSTEM_LOG_ENABLED */
  def setSystemLogEnabled(enabled: Boolean): Config =
    conf.setProperty(SYSTEM_LOG_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS */
  def setSlowInvocationDetectorThreshold(threshold: FiniteDuration): Config =
    conf.setProperty(SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS, threshold.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.SERIALIZATION_VERSION */
  def setSerializationVersion(version: Byte): Config =
    conf.setProperty(SERIALIZATION_VERSION, version.toString)
  /** @see com.hazelcast.instance.GroupProperty.QUERY_PREDICATE_PARALLEL_EVALUATION */
  def setQueryPredicateParallelEvaluation(enabled: Boolean): Config =
    conf.setProperty(QUERY_PREDICATE_PARALLEL_EVALUATION, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.QUERY_OPTIMIZER_TYPE */
  def setQueryOptimizerType(typ: QueryOptimizerFactory.Type): Config =
    conf.setProperty(QUERY_OPTIMIZER_TYPE, typ.name)
  /** @see com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_DELAY_SECONDS */
  def setPerfMonDelay(delay: FiniteDuration): Config =
    conf.setProperty(PERFORMANCE_MONITOR_DELAY_SECONDS, delay.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_ENABLED */
  def setPerfMonEnabled(enabled: Boolean): Config =
    conf.setProperty(PERFORMANCE_MONITOR_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT */
  def setPerfMonHumanFormatEnabled(enabled: Boolean): Config =
    conf.setProperty(PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT */
  def setPerfMonMaxRolledFileCount(count: Int): Config =
    conf.setProperty(PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT, count.toString)
  /** @see com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB */
  def setPerfMonMaxRolledFileSize(maxSize: MemorySize): Config =
    conf.setProperty(PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB, maxSize.megaBytes.toString)
  /** @see com.hazelcast.instance.GroupProperty.PERFORMANCE_METRICS_LEVEL */
  def setPerformanceMetricsLevel(level: ProbeLevel): Config =
    conf.setProperty(PERFORMANCE_METRICS_LEVEL, level.name)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_MIGRATION_ZIP_ENABLED */
  def setPartitionMigrationZipEnabled(enabled: Boolean): Config =
    conf.setProperty(PARTITION_MIGRATION_ZIP_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS */
  def setLockMaxLeaseTime(maxLease: FiniteDuration): Config =
    conf.setProperty(LOCK_MAX_LEASE_TIME_SECONDS, maxLease.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.DISCOVERY_SPI_ENABLED */
  def setDiscoverySPIEnabled(enabled: Boolean): Config =
    conf.setProperty(DISCOVERY_SPI_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED */
  def setDiscoverySPIPublicIPEnabled(enabled: Boolean): Config =
    conf.setProperty(DISCOVERY_SPI_PUBLIC_IP_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS */
  def setClientHeartbeatTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(CLIENT_HEARTBEAT_TIMEOUT_SECONDS, timeout.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS */
  def setClusterShutdownTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(CLUSTER_SHUTDOWN_TIMEOUT_SECONDS, timeout.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED */
  def setCacheInvalidationBatchEnabled(enabled: Boolean): Config =
    conf.setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE */
  def setCacheInvalidationBatchSize(size: Int): Config =
    conf.setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE, size.toString)
  /** @see com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS */
  def setCacheInvalidationBatchFrequency(freq: FiniteDuration): Config =
    conf.setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, freq.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.APPLICATION_VALIDATION_TOKEN */
  def setApplicationValidationToken(token: String): Config =
    conf.setProperty(APPLICATION_VALIDATION_TOKEN, token)
  /** @see com.hazelcast.instance.GroupProperty.BACKPRESSURE_ENABLED */
  def setBackpressureEnabled(enabled: Boolean): Config =
    conf.setProperty(BACKPRESSURE_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS */
  def setBackpressureBackoffTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS, timeout.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION */
  def setBackpressureMaxConcurrentInvocationsPerPartition(max: Int): Config =
    conf.setProperty(BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION, max.toString)
  /** @see com.hazelcast.instance.GroupProperty.BACKPRESSURE_SYNCWINDOW */
  def setBackpressureSyncWindow(window: Int): Config =
    conf.setProperty(BACKPRESSURE_SYNCWINDOW, window.toString)
  /** @see com.hazelcast.instance.GroupProperty.CLIENT_ENGINE_THREAD_COUNT */
  def setClientEngineThreadCount(size: Int): Config =
    conf.setProperty(CLIENT_ENGINE_THREAD_COUNT, size.toString)
  /** @see com.hazelcast.instance.GroupProperty.CONNECT_ALL_WAIT_SECONDS */
  def setConnectAllWait(wait: FiniteDuration): Config =
    conf.setProperty(CONNECT_ALL_WAIT_SECONDS, wait.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.CONNECTION_MONITOR_INTERVAL */
  def setConnectionMonitorInterval(interval: FiniteDuration): Config =
    conf.setProperty(CONNECTION_MONITOR_INTERVAL, interval.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.CONNECTION_MONITOR_MAX_FAULTS */
  def setConnectionMonitorMaxFaults(max: Int): Config =
    conf.setProperty(CONNECTION_MONITOR_MAX_FAULTS, max.toString)
  def setLicenseKey(key: String): Config =
    conf.setProperty(ENTERPRISE_LICENSE_KEY, key)
  /** @see com.hazelcast.instance.GroupProperty.EVENT_QUEUE_CAPACITY */
  def setEventQueueCapacity(capacity: Int): Config =
    conf.setProperty(EVENT_QUEUE_CAPACITY, capacity.toString)
  /** @see com.hazelcast.instance.GroupProperty.EVENT_QUEUE_TIMEOUT_MILLIS */
  def setEventQueueTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(EVENT_QUEUE_TIMEOUT_MILLIS, timeout.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.EVENT_THREAD_COUNT */
  def setEventThreadCount(threads: Int): Config =
    conf.setProperty(EVENT_THREAD_COUNT, threads.toString)
  /** @see com.hazelcast.instance.GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT */
  def setGracefulShutdownMaxWait(maxWait: FiniteDuration): Config =
    conf.setProperty(GRACEFUL_SHUTDOWN_MAX_WAIT, maxWait.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.HEALTH_MONITORING_DELAY_SECONDS */
  def setHealthMonitoringInterval(interval: FiniteDuration): Config =
    conf.setProperty(HEALTH_MONITORING_DELAY_SECONDS, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.HEALTH_MONITORING_LEVEL */
  def setHealthMonitoringLevel(level: HealthMonitorLevel): Config =
    conf.setProperty(HEALTH_MONITORING_LEVEL, level.name)
  /** @see com.hazelcast.instance.GroupProperty.HEARTBEAT_INTERVAL_SECONDS */
  def setHeartbeatInterval(interval: FiniteDuration): Config =
    conf.setProperty(HEARTBEAT_INTERVAL_SECONDS, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.ICMP_ENABLED */
  def setIcmpEnabled(enabled: Boolean): Config =
    conf.setProperty(ICMP_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.ICMP_TIMEOUT */
  def setIcmpTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(ICMP_TIMEOUT, timeout.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.ICMP_TTL */
  def setIcmpTTL(hops: Int): Config =
    conf.setProperty(ICMP_TTL, hops.toString)
  /** @see com.hazelcast.instance.GroupProperty.INITIAL_MIN_CLUSTER_SIZE */
  def setInitialMinClusterSize(minSize: Int): Config =
    conf.setProperty(INITIAL_MIN_CLUSTER_SIZE, minSize.toString)
  /** @see com.hazelcast.instance.GroupProperty.INITIAL_WAIT_SECONDS */
  def setInitialWait(wait: FiniteDuration): Config =
    conf.setProperty(INITIAL_WAIT_SECONDS, wait.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.IO_BALANCER_INTERVAL_SECONDS */
  def setIOBalancerInterval(interval: FiniteDuration): Config =
    conf.setProperty(IO_BALANCER_INTERVAL_SECONDS, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.IO_THREAD_COUNT */
  def setIOThreadCount(threads: Int): Config =
    conf.setProperty(IO_THREAD_COUNT, threads.toString)
  /** @see com.hazelcast.instance.GroupProperty.JCACHE_PROVIDER_TYPE */
  def setJCacheProviderType(providerType: String): Config =
    conf.setProperty(JCACHE_PROVIDER_TYPE, providerType)
  /** @see com.hazelcast.instance.GroupProperty.ENABLE_JMX */
  def setJmxEnabled(enabled: Boolean): Config =
    conf.setProperty(ENABLE_JMX, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.ENABLE_JMX_DETAILED */
  def setJmxDetailed(detailed: Boolean): Config =
    conf.setProperty(ENABLE_JMX_DETAILED, detailed.toString)
  /** @see com.hazelcast.instance.GroupProperty.LOGGING_TYPE */
  def setLoggingFramework(fw: String): Config =
    conf.setProperty(LOGGING_TYPE, fw)
  /** @see com.hazelcast.instance.GroupProperty.MAP_EXPIRY_DELAY_SECONDS */
  def setMapExpiryDelay(delay: FiniteDuration): Config =
    conf.setProperty(MAP_EXPIRY_DELAY_SECONDS, delay.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAP_LOAD_CHUNK_SIZE */
  def setMapLoadChunkSize(size: Int): Config =
    conf.setProperty(MAP_LOAD_CHUNK_SIZE, size.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS */
  def setMapReplicaDelay(delay: FiniteDuration): Config =
    conf.setProperty(MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS, delay.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY */
  def setMapWriteBehindQueueCapacity(capacity: Int): Config =
    conf.setProperty(MAP_WRITE_BEHIND_QUEUE_CAPACITY, capacity.toString)
  /** @see com.hazelcast.instance.GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS */
  def setMasterConfirmationInterval(interval: FiniteDuration): Config =
    conf.setProperty(MASTER_CONFIRMATION_INTERVAL_SECONDS, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAX_JOIN_MERGE_TARGET_SECONDS */
  def setMergeTargetTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(MAX_JOIN_MERGE_TARGET_SECONDS, timeout.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAX_JOIN_SECONDS */
  def setJoinTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(MAX_JOIN_SECONDS, timeout.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAX_NO_HEARTBEAT_SECONDS */
  def setHeartbeatTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(MAX_NO_HEARTBEAT_SECONDS, timeout.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS */
  def setMasterConfirmationTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS, timeout.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAX_WAIT_SECONDS_BEFORE_JOIN */
  def setMaxWaitBeforeJoin(wait: FiniteDuration): Config =
    conf.setProperty(MAX_WAIT_SECONDS_BEFORE_JOIN, wait.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MC_MAX_VISIBLE_INSTANCE_COUNT */
  def setManCenterMaxVisibleInstances(count: Int): Config =
    conf.setProperty(MC_MAX_VISIBLE_INSTANCE_COUNT, count.toString)
  /** @see com.hazelcast.instance.GroupProperty.MC_MAX_VISIBLE_SLOW_OPERATION_COUNT */
  def setManCenterMaxVisibleSlowOps(count: Int): Config =
    conf.setProperty(MC_MAX_VISIBLE_SLOW_OPERATION_COUNT, count.toString)
  /** @see com.hazelcast.instance.GroupProperty.MC_URL_CHANGE_ENABLED */
  def setManCenterUrlChangeEnabled(enabled: Boolean): Config =
    conf.setProperty(MC_URL_CHANGE_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS */
  def setMemberListPublishInterval(interval: FiniteDuration): Config =
    conf.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MEMCACHE_ENABLED */
  def setMemcacheEnabled(enabled: Boolean): Config =
    conf.setProperty(MEMCACHE_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS */
  def setMergeRunDelay(delay: FiniteDuration): Config =
    conf.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS, delay.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS */
  def setMergeRunInterval(interval: FiniteDuration): Config =
    conf.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS */
  def setMemberRemovedMigrationDelay(delay: FiniteDuration): Config =
    conf.setProperty(MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS, delay.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS */
  def setOperationBackupTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(OPERATION_BACKUP_TIMEOUT_MILLIS, timeout.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS */
  def setOperationCallTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(OPERATION_CALL_TIMEOUT_MILLIS, timeout.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.GENERIC_OPERATION_THREAD_COUNT */
  def setGenericOperationThreadCount(threads: Int): Config =
    conf.setProperty(GENERIC_OPERATION_THREAD_COUNT, threads.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_OPERATION_THREAD_COUNT */
  def setPartitionOperationThreadCount(threads: Int): Config =
    conf.setProperty(PARTITION_OPERATION_THREAD_COUNT, threads.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL */
  def setPartitionBackupSyncInterval(interval: FiniteDuration): Config =
    conf.setProperty(PARTITION_BACKUP_SYNC_INTERVAL, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_COUNT */
  def setPartitionCount(partitions: Int): Config =
    conf.setProperty(PARTITION_COUNT, partitions.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS */
  def setPartitionMaxParallelReplications(max: Int): Config =
    conf.setProperty(PARTITION_MAX_PARALLEL_REPLICATIONS, max.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_MIGRATION_INTERVAL */
  def setPartitionMigrationInterval(interval: FiniteDuration): Config =
    conf.setProperty(PARTITION_MIGRATION_INTERVAL, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_MIGRATION_TIMEOUT */
  def setPartitionMigrationTimeout(timeout: FiniteDuration): Config =
    conf.setProperty(PARTITION_MIGRATION_TIMEOUT, timeout.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITION_TABLE_SEND_INTERVAL */
  def setPartitionTablePublishInterval(interval: FiniteDuration): Config =
    conf.setProperty(PARTITION_TABLE_SEND_INTERVAL, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.PARTITIONING_STRATEGY_CLASS */
  def setPartitioningStrategy(cls: Class[_ <: PartitioningStrategy[_]]): Config =
    conf.setProperty(PARTITIONING_STRATEGY_CLASS, cls.getName)
  /** @see com.hazelcast.instance.GroupProperty.PREFER_IPv4_STACK */
  def setPreferIPv4(prefer: Boolean): Config =
    conf.setProperty(PREFER_IPv4_STACK, prefer.toString)
  /** @see com.hazelcast.instance.GroupProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK */
  def setQueryMaxLocalPartitionPreCheckLimit(limit: Int): Config =
    conf.setProperty(QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK, limit.toString)
  /** @see com.hazelcast.instance.GroupProperty.QUERY_RESULT_SIZE_LIMIT */
  def setQueryResultSizeLimit(limit: Int): Config =
    conf.setProperty(QUERY_RESULT_SIZE_LIMIT, limit.toString)
  /** @see com.hazelcast.instance.GroupProperty.REST_ENABLED */
  def setRESTEnabled(enabled: Boolean): Config =
    conf.setProperty(REST_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.SHUTDOWNHOOK_ENABLED */
  def setShutdownHookEnabled(enabled: Boolean): Config =
    conf.setProperty(SHUTDOWNHOOK_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_ENABLED */
  def setSlowOpsDetectorEnabled(enabled: Boolean): Config =
    conf.setProperty(SLOW_OPERATION_DETECTOR_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS */
  def setSlowOpsDetectorLogPurgeInterval(interval: FiniteDuration): Config =
    conf.setProperty(SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS, interval.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS */
  def setSlowOpsDetectorLogRetention(retention: FiniteDuration): Config =
    conf.setProperty(SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS, retention.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED */
  def setSlowOpsDetectorStackTraceLoggingEnabled(enabled: Boolean): Config =
    conf.setProperty(SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS */
  def setSlowOpsDetectorThreshold(threshold: FiniteDuration): Config =
    conf.setProperty(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, threshold.toMillis.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_BIND_ANY */
  def setSocketBindAny(any: Boolean): Config =
    conf.setProperty(SOCKET_BIND_ANY, any.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_CLIENT_BIND */
  def setSocketClientBind(bind: Boolean): Config =
    conf.setProperty(SOCKET_CLIENT_BIND, bind.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_CLIENT_BIND_ANY */
  def setSocketClientBindAny(any: Boolean): Config =
    conf.setProperty(SOCKET_CLIENT_BIND_ANY, any.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_CLIENT_RECEIVE_BUFFER_SIZE */
  def setSocketClientReceiveBufferSize(size: MemorySize): Config =
    conf.setProperty(SOCKET_CLIENT_RECEIVE_BUFFER_SIZE, size.kiloBytes.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_CLIENT_SEND_BUFFER_SIZE */
  def setSocketClientSendBufferSize(size: MemorySize): Config =
    conf.setProperty(SOCKET_CLIENT_SEND_BUFFER_SIZE, size.kiloBytes.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_CONNECT_TIMEOUT_SECONDS */
  def setSocketConnectTimeout(timeout: Duration): Config =
    conf.setProperty(SOCKET_CONNECT_TIMEOUT_SECONDS, (if (timeout.isFinite) timeout.toSeconds else 0).toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_KEEP_ALIVE */
  def setSocketKeepAlive(keepAlive: Boolean): Config =
    conf.setProperty(SOCKET_KEEP_ALIVE, keepAlive.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_LINGER_SECONDS */
  def setSocketLinger(linger: FiniteDuration): Config =
    conf.setProperty(SOCKET_LINGER_SECONDS, linger.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_NO_DELAY */
  def setSocketNoDelay(noDelay: Boolean): Config =
    conf.setProperty(SOCKET_NO_DELAY, noDelay.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE */
  def setSocketReceiveBufferSize(size: MemorySize): Config =
    conf.setProperty(SOCKET_RECEIVE_BUFFER_SIZE, size.kiloBytes.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_SEND_BUFFER_SIZE */
  def setSocketSendBufferSize(size: MemorySize): Config =
    conf.setProperty(SOCKET_SEND_BUFFER_SIZE, size.kiloBytes.toString)
  /** @see com.hazelcast.instance.GroupProperty.SOCKET_SERVER_BIND_ANY */
  def setSocketServerBindAny(any: Boolean): Config =
    conf.setProperty(SOCKET_SERVER_BIND_ANY, any.toString)
  /** @see com.hazelcast.instance.GroupProperty.TCP_JOIN_PORT_TRY_COUNT */
  def setTcpJoinPortTryCount(count: Int): Config =
    conf.setProperty(TCP_JOIN_PORT_TRY_COUNT, count.toString)
  /** @see com.hazelcast.instance.GroupProperty.WAIT_SECONDS_BEFORE_JOIN */
  def setWaitBeforeJoin(wait: FiniteDuration): Config =
    conf.setProperty(WAIT_SECONDS_BEFORE_JOIN, wait.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty.PHONE_HOME_ENABLED */
  def setPhoneHomeEnabled(enabled: Boolean): Config =
    conf.setProperty(PHONE_HOME_ENABLED, enabled.toString)

  /** @see com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED */
  def setIMapNearCacheInvalidationBatchEnabled(enabled: Boolean): Config =
    conf.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE */
  def setIMapNearCacheInvalidationBatchSize(size: Int): Config =
    conf.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE, size.toString)
  /** @see com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS */
  def setIMapNearCacheInvalidationBatchFrequency(freq: FiniteDuration): Config =
    conf.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, freq.toSeconds.toString)
  /** @see com.hazelcast.instance.GroupProperty. */

  /** @see com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED */
  def setJCacheNearCacheInvalidationBatchEnabled(enabled: Boolean): Config =
    conf.setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED, enabled.toString)
  /** @see com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE */
  def setJCacheNearCacheInvalidationBatchSize(size: Int): Config =
    conf.setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE, size.toString)
  /** @see com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS */
  def setJCacheNearCacheInvalidationBatchFrequency(freq: FiniteDuration): Config =
    conf.setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, freq.toSeconds.toString)
}
