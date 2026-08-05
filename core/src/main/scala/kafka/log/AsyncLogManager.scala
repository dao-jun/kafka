/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.metadata.{ConfigRepository, MetadataCache}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.bookkeeper.{BookkeeperLocalLog, BookkeeperStorage, BookkeeperUnifiedLog}
import org.apache.kafka.storage.internals.log.{AsyncProducerStateManager, AsyncTransactionIndex, CleanerConfig, LogCleaner, LogConfig, LogDirFailureChannel, LogOffsetsListener, ProducerStateManagerConfig, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.apache.bookkeeper.mledger.{AsyncCallbacks, ManagedLedgerException}

import java.io.File
import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, ConcurrentMap, CountDownLatch}
import scala.jdk.CollectionConverters._
import scala.collection._

class AsyncLogManager(brokerId: Int,
                      logDirs: Seq[File],
                      initialOfflineDirs: Seq[File],
                      configRepository: ConfigRepository,
                      override val initialDefaultConfig: LogConfig,
                      override val cleanerConfig: CleanerConfig,
                      recoveryThreadsPerDataDir: Int,
                      override val flushCheckMs: Long,
                      override val flushRecoveryOffsetCheckpointMs: Long,
                      override val flushStartOffsetCheckpointMs: Long,
                      override val retentionCheckMs: Long,
                      override val maxTransactionTimeoutMs: Int,
                      override val producerStateManagerConfig: ProducerStateManagerConfig,
                      override val producerIdExpirationCheckIntervalMs: Int,
                      scheduler: Scheduler,
                      brokerTopicStats: BrokerTopicStats,
                      logDirFailureChannel: LogDirFailureChannel,
                      time: Time,
                      remoteStorageSystemEnable: Boolean,
                      override val initialTaskDelayMs: Long,
                      cleanerFactory: (CleanerConfig, util.List[File], ConcurrentMap[TopicPartition, UnifiedLog], LogDirFailureChannel, Time) => LogCleaner)
  extends LogManager(
    logDirs,
    initialOfflineDirs,
    configRepository,
    initialDefaultConfig,
    cleanerConfig,
    recoveryThreadsPerDataDir,
    flushCheckMs,
    flushRecoveryOffsetCheckpointMs,
    flushStartOffsetCheckpointMs,
    retentionCheckMs,
    maxTransactionTimeoutMs,
    producerStateManagerConfig,
    producerIdExpirationCheckIntervalMs,
    scheduler,
    brokerTopicStats,
    logDirFailureChannel,
    time,
    remoteStorageSystemEnable,
    initialTaskDelayMs,
    cleanerFactory) with Logging {

  private val bookkeeperStorageOpt: Option[BookkeeperStorage] =
    if (initialDefaultConfig.asyncLogModeEnable) {
      Some(new BookkeeperStorage(initialDefaultConfig))
    } else {
      None
    }

  override def asyncLogModeEnabled: Boolean = true

  private val asyncLogs = new util.concurrent.ConcurrentHashMap[TopicPartition, CompletableFuture[BookkeeperUnifiedLog]]()

  private val metadataCacheOpt: Option[MetadataCache] = configRepository match {
    case metadataCache: MetadataCache =>
      Some(metadataCache)
    case _ =>
      None
  }

  private val shutdown: AtomicBoolean = new AtomicBoolean(false)


  override def startup(topicNames: Set[String], isStray: UnifiedLog => Boolean): Unit = {}

  override def getLog(topicPartition: TopicPartition, isFuture: Boolean): Option[UnifiedLog] = {
    val logFuture = asyncLogs.get(topicPartition)
    if (logFuture != null) {
      if (logFuture.isDone) {
        return Some(logFuture.join())
      } else {
        return None
      }
    }
    None
  }

  override def getOrCreateLog(topicPartition: TopicPartition, isNew: Boolean, isFuture: Boolean,
                              topicId: Optional[Uuid], targetLogDirectoryId: Option[Uuid]): UnifiedLog = {
    throw new UnsupportedOperationException("AsyncLogManager does not support getOrCreateLog")
  }


  override def getOrCreateLogAsync(topicPartition: TopicPartition, isNew: Boolean, isFuture: Boolean,
                                   topicId: Optional[Uuid], targetLogDirectoryId: Option[Uuid]): CompletableFuture[BookkeeperUnifiedLog] = {
    // 参数验证...
    if (bookkeeperStorageOpt.isEmpty) {
      return CompletableFuture.failedFuture(
        new IllegalStateException("BookkeeperStorageSingleton is not initialized for async log mode"))
    }

    // 检查是否本地托管...
    val isLocalHosted = metadataCacheOpt match {
      case Some(cache) =>
        val leaderAndIsr = cache.getLeaderAndIsr(topicPartition.topic(), topicPartition.partition())
        if (leaderAndIsr.isEmpty) {
          return CompletableFuture.failedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception())
        }
        val leader = leaderAndIsr.get.leader
        leader == brokerId
      case None =>
        true
    }

    if (!isLocalHosted) {
      return CompletableFuture.failedFuture(Errors.NOT_LEADER_OR_FOLLOWER.exception())
    }

    // 使用 computeIfAbsent 原子性地获取或创建
    asyncLogs.computeIfAbsent(topicPartition, _ => {
      val future = new CompletableFuture[BookkeeperUnifiedLog]()

      // 异步初始化
      initializeLogAsync(topicPartition, topicId, future)

      future
    })
  }

  private def initializeLogAsync(topicPartition: TopicPartition,
                                 topicId: Optional[Uuid],
                                 future: CompletableFuture[BookkeeperUnifiedLog]): Unit = {
    val bookkeeperStorage = bookkeeperStorageOpt.get
    val transactionIndex = new AsyncTransactionIndex(bookkeeperStorage.getMetadataStoreExtended, topicPartition)
    val producerStateManager = new AsyncProducerStateManager(topicPartition, maxTransactionTimeoutMs,
      producerStateManagerConfig, time, bookkeeperStorage.getMetadataStoreExtended)

    val localLog = new BookkeeperLocalLog(initialDefaultConfig, scheduler, topicPartition, transactionIndex)

    localLog.initializeAsync(bookkeeperStorage.getManagedLedgerFactory)
      .thenApply(startOffset => {
        try {
          val bookkeeperUnifiedLog = new BookkeeperUnifiedLog(
            localLog.logStartOffset(),
            localLog,
            brokerTopicStats,
            producerIdExpirationCheckIntervalMs,
            null,
            producerStateManager,
            topicId,
            false,
            LogOffsetsListener.NO_OP_OFFSETS_LISTENER
          )
          info(s"Initialized BookkeeperUnifiedLog for $topicPartition , logStartOffset: $startOffset, logEndOffset: ${localLog.logEndOffset()}")
          bookkeeperUnifiedLog
        } catch {
          case t: Throwable =>
            error(s"Failed to initialize log for $topicPartition", t)
            throw Errors.NOT_LEADER_OR_FOLLOWER.exception()
        }
      })
      .thenCompose(bookkeeperUnifiedLog => {
        bookkeeperUnifiedLog.initialize()
      })
      .thenAccept(bookkeeperUnifiedLog => {
        future.complete(bookkeeperUnifiedLog)
      })
      .exceptionally(t => {
        error(s"Failed to initialize log for $topicPartition", t)

        // 初始化失败时从 map 中移除，允许后续重试
        asyncLogs.remove(topicPartition, future)
        future.completeExceptionally(t)
        null
      })
  }


  override def shutdown(brokerEpoch: Long): Unit = {
    if (!shutdown.compareAndSet(false, true)) {
      return
    }
    asyncLogs.values().forEach(logFuture => {
      logFuture.thenAccept(log => {
        log.close()
      })
    })
    asyncLogs.clear()
  }

  override def directoryIdsSet: Predef.Set[Uuid] = {
    val set = mutable.Set[Uuid]()
    set.add(new Uuid(brokerId, brokerId))
    immutable.Set.from(set)
  }

  override def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    // noop, for ISR
  }

  override def truncateFullyAndStartAt(topicPartition: TopicPartition,
                                       newOffset: Long,
                                       isFuture: Boolean,
                                       logStartOffsetOpt: Optional[java.lang.Long]): Unit = {
    // noop, for ISR
  }

  override def checkpointLogRecoveryOffsets(): Unit = {
    // noop
  }

  override def checkpointLogStartOffsets(): Unit = {
    // noop
  }

  override def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // noop
  }

  override def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    // noop
  }

  override def abortCleaning(topicPartition: TopicPartition): Unit = {
    // noop
  }

  override def resumeCleaning(topicPartition: TopicPartition): Unit = {
    // noop
  }

  override def updateTopicConfig(topic: String,
                                 newTopicConfig: java.util.Properties,
                                 isRemoteLogStorageSystemEnabled: Boolean,
                                 wasRemoteLogEnabled: Boolean): Unit = {
    // TODO
    // 实现这个方法，更新Log Config
  }

  override def topicConfigUpdated(topic: String): Unit = {
    // noop
  }

  override def brokerConfigUpdated(): Unit = {
    // TODO
    // 或许应该实现
  }

  override def initializingLog(topicPartition: TopicPartition): Unit = {
    // noop
  }

  override def finishedInitializingLog(topicPartition: TopicPartition, maybeLog: Option[UnifiedLog]): Unit = {
    // noop
  }

  override def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    // noop
  }

  override def replaceCurrentWithFutureLog(sourceLog: Option[UnifiedLog], destLog: UnifiedLog, updateHighWatermark: Boolean): Unit = {
    // noop
  }

  override def asyncDelete(topicPartition: TopicPartition,
                           isFuture: Boolean,
                           checkpoint: Boolean,
                           isStray: Boolean): Option[UnifiedLog] = {
    val logFuture = asyncLogs.remove(topicPartition)
    if (logFuture == null || !logFuture.isDone || logFuture.isCompletedExceptionally) {
      return None
    }
    val log = logFuture.join()
    log.close()

    val name = topicPartition.topic() + "-" + topicPartition.partition()
    bookkeeperStorageOpt.foreach(bookkeeperStorage => {
      bookkeeperStorage.getManagedLedgerFactory.asyncDelete(name, new AsyncCallbacks.DeleteLedgerCallback {

        override def deleteLedgerComplete(ctx: Any): Unit = {
          info(s"Deleted log for $topicPartition")
        }

        override def deleteLedgerFailed(exception: ManagedLedgerException, ctx: Any): Unit = {
          error(s"Failed to delete log for $topicPartition", exception)
        }
      }, null)
    })

    Some(log)
  }

  override def asyncDelete(topicPartitions: Iterable[TopicPartition],
                           isStray: Boolean,
                           errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    val latch = new CountDownLatch(topicPartitions.size)
    topicPartitions.foreach(topicPartition => {
      val logFuture = asyncLogs.remove(topicPartition)
      if (logFuture != null && logFuture.isDone && !logFuture.isCompletedExceptionally) {
        val log = logFuture.join()
        log.closeAsync().thenAccept(_ => {
          val name = topicPartition.topic() + "-" + topicPartition.partition()
          bookkeeperStorageOpt.foreach(bookkeeperStorage => {
            bookkeeperStorage.getManagedLedgerFactory.asyncDelete(name, new AsyncCallbacks.DeleteLedgerCallback {
              override def deleteLedgerComplete(ctx: Any): Unit = {
                info(s"Deleted log for $topicPartition")
                latch.countDown()
              }

              override def deleteLedgerFailed(exception: ManagedLedgerException, ctx: Any): Unit = {
                errorHandler(topicPartition, exception)
                latch.countDown()
              }
            }, null)
          })
        })
      } else {
        latch.countDown()
      }
    })

    try {
      latch.await()
    } catch {
      case e: Throwable =>
        error(s"Failed to delete logs for $topicPartitions", e)
    }
  }

  override def allLogs: Iterable[UnifiedLog] = {
    val logs = new util.ArrayList[UnifiedLog]()
    asyncLogs.values().forEach(f => if (f.isDone && !f.isCompletedExceptionally) {
      logs.add(f.join())
    })
    logs.asScala
  }

  override def logsByTopic(topic: String): Seq[UnifiedLog] = {
    val logs = new util.ArrayList[UnifiedLog]()
    asyncLogs.entrySet().forEach(entry => {
      val topicPartition = entry.getKey
      if (topicPartition.topic == topic) {
        if (entry.getValue.isDone && !entry.getValue.isCompletedExceptionally) {
          logs.add(entry.getValue.join())
        }
      }
    })
    logs.asScala
  }

  override def isLogDirOnline(logDir: String): Boolean = {
    true
  }

  override def readBrokerEpochFromCleanShutdownFiles(): java.util.OptionalLong = {
    java.util.OptionalLong.of(0L)
  }

  override def liveLogDirs: Seq[File] = {
    Seq.empty
  }

  override def handleLogDirFailure(dir: String): Unit = {
    // noop
  }

  override def resizeRecoveryThreadPool(newSize: Int): Unit = {
    // noop
  }

}

object AsyncLogManager {
  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            configRepository: ConfigRepository,
            kafkaScheduler: Scheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel): AsyncLogManager = {
    val defaultProps = config.extractLogConfigMap

    LogConfig.validateBrokerLogConfigValues(defaultProps, config.remoteLogManagerConfig.isRemoteStorageSystemEnabled)
    val defaultLogConfig = new LogConfig(defaultProps)

    val cleanerConfig = new CleanerConfig(config)
    val transactionLogConfig = new TransactionLogConfig(config)

    new AsyncLogManager(config.brokerId(),
      logDirs = config.logDirs.asScala.map(new File(_).getAbsoluteFile),
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      configRepository = configRepository,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxTransactionTimeoutMs = new TransactionStateManagerConfig(config).transactionMaxTimeoutMs,
      producerStateManagerConfig = new ProducerStateManagerConfig(transactionLogConfig.producerIdExpirationMs, transactionLogConfig.transactionPartitionVerificationEnable),
      producerIdExpirationCheckIntervalMs = transactionLogConfig.producerIdExpirationCheckIntervalMs,
      scheduler = kafkaScheduler,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time,
      remoteStorageSystemEnable = config.remoteLogManagerConfig.isRemoteStorageSystemEnabled,
      initialTaskDelayMs = config.logInitialTaskDelayMs,
      cleanerFactory = (cleanerConfig, files, map, logDirFailureChannel, time) =>
        new LogCleaner(cleanerConfig, files, map, logDirFailureChannel, time))
  }
}