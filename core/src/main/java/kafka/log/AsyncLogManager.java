/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log;

import kafka.server.KafkaConfig;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.AsyncProducerStateManager;
import org.apache.kafka.storage.internals.log.AsyncTransactionIndex;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogCleaner;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogManager;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.bookkeeper.BookkeeperLocalLog;
import org.apache.kafka.storage.internals.log.bookkeeper.BookkeeperStorage;
import org.apache.kafka.storage.internals.log.bookkeeper.BookkeeperUnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Async, BookKeeper-backed implementation of {@link LogManager}.
 *
 * <p>This is a faithful Java rewrite of the original Scala {@code AsyncLogManager} from the
 * bk-kafka branch. The Scala version extended the (now-deleted) Scala {@code kafka.log.LogManager}
 * class; on trunk that class was migrated to Java ({@link LogManager}), so this Java rewrite
 * extends the Java parent directly — avoiding the {@code override val} / private-field
 * "split-brain" problem that would occur if a Scala subclass tried to override Java fields.
 *
 * <p>Method names, parameters, and logic are kept strictly equivalent to the original Scala
 * version to ease review.
 */
public class AsyncLogManager extends LogManager {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncLogManager.class);

    private final int brokerId;
    private final Scheduler scheduler;
    private final BrokerTopicStats brokerTopicStats;
    private final Time time;

    // Original Scala: private val bookkeeperStorageOpt: Option[BookkeeperStorage]
    private final BookkeeperStorage bookkeeperStorageOpt;

    // Original Scala: private val asyncLogs = new ConcurrentHashMap[...]()
    private final ConcurrentHashMap<TopicPartition, CompletableFuture<BookkeeperUnifiedLog>> asyncLogs =
        new ConcurrentHashMap<>();

    // Original Scala: private val metadataCacheOpt: Option[MetadataCache]
    private final MetadataCache metadataCacheOpt;

    // Original Scala: private val shutdown: AtomicBoolean
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * Original Scala constructor (lines 41-82 of AsyncLogManager.scala). Translated to Java;
     * the {@code override val} params become plain fields passed to the parent constructor.
     */
    public AsyncLogManager(int brokerId,
                           List<File> logDirs,
                           List<File> initialOfflineDirs,
                           ConfigRepository configRepository,
                           LogConfig initialDefaultConfig,
                           CleanerConfig cleanerConfig,
                           int recoveryThreadsPerDataDir,
                           long flushCheckMs,
                           long flushRecoveryOffsetCheckpointMs,
                           long flushStartOffsetCheckpointMs,
                           long retentionCheckMs,
                           int maxTransactionTimeoutMs,
                           ProducerStateManagerConfig producerStateManagerConfig,
                           int producerIdExpirationCheckIntervalMs,
                           Scheduler scheduler,
                           BrokerTopicStats brokerTopicStats,
                           LogDirFailureChannel logDirFailureChannel,
                           Time time,
                           boolean remoteStorageSystemEnable,
                           long initialTaskDelayMs,
                           LogCleanerBuilder cleanerBuilder) throws Exception {
        super(logDirs, initialOfflineDirs, configRepository, initialDefaultConfig, cleanerConfig,
            recoveryThreadsPerDataDir, flushCheckMs, flushRecoveryOffsetCheckpointMs,
            flushStartOffsetCheckpointMs, retentionCheckMs, maxTransactionTimeoutMs,
            producerStateManagerConfig, producerIdExpirationCheckIntervalMs, scheduler,
            brokerTopicStats, logDirFailureChannel, time, remoteStorageSystemEnable,
            initialTaskDelayMs, cleanerBuilder);
        this.brokerId = brokerId;
        this.scheduler = scheduler;
        this.brokerTopicStats = brokerTopicStats;
        this.time = time;

        // Original Scala lines 84-89: bookkeeperStorageOpt init
        this.bookkeeperStorageOpt = initialDefaultConfig.asyncLogModeEnable
            ? new BookkeeperStorage(initialDefaultConfig)
            : null;

        // Original Scala lines 95-100: metadataCacheOpt init
        this.metadataCacheOpt = (configRepository instanceof MetadataCache)
            ? (MetadataCache) configRepository
            : null;
    }

    // Original Scala line 91: override def asyncLogModeEnabled: Boolean = true
    @Override
    public boolean asyncLogModeEnabled() {
        return true;
    }

    // Original Scala line 105: override def startup(...) = {}
    @Override
    public void startup(Set<String> topicNames, Function<UnifiedLog, Boolean> isStray) {
        // noop — bookkeeper logs are initialized lazily via getOrCreateLogAsync
    }

    // Original Scala lines 107-117: override def getLog(...)
    @Override
    public Optional<UnifiedLog> getLog(TopicPartition topicPartition, boolean isFuture) {
        CompletableFuture<BookkeeperUnifiedLog> logFuture = asyncLogs.get(topicPartition);
        if (logFuture != null) {
            if (logFuture.isDone()) {
                return Optional.of(logFuture.join());
            } else {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    // Original Scala lines 119-122: override def getOrCreateLog(...)
    @Override
    public UnifiedLog getOrCreateLog(TopicPartition topicPartition, boolean isNew, boolean isFuture,
                                     Optional<Uuid> topicId, Optional<Uuid> targetLogDirectoryId) {
        throw new UnsupportedOperationException("AsyncLogManager does not support getOrCreateLog");
    }

    // Original Scala lines 125-159: override def getOrCreateLogAsync(...)
    @Override
    public CompletableFuture<BookkeeperUnifiedLog> getOrCreateLogAsync(TopicPartition topicPartition,
                                                                       boolean isNew,
                                                                       boolean isFuture,
                                                                       Optional<Uuid> topicId,
                                                                       Optional<Uuid> targetLogDirectoryId) {
        // 参数验证 — bookkeeperStorageOpt.isEmpty
        if (bookkeeperStorageOpt == null) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("BookkeeperStorageSingleton is not initialized for async log mode"));
        }

        // 检查是否本地托管
        boolean isLocalHosted;
        if (metadataCacheOpt != null) {
            Optional<LeaderAndIsr> leaderAndIsr = metadataCacheOpt.getLeaderAndIsr(topicPartition.topic(), topicPartition.partition());
            if (leaderAndIsr.isEmpty()) {
                return CompletableFuture.failedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception());
            }
            int leader = leaderAndIsr.get().leader();
            isLocalHosted = (leader == brokerId);
        } else {
            isLocalHosted = true;
        }

        if (!isLocalHosted) {
            return CompletableFuture.failedFuture(Errors.NOT_LEADER_OR_FOLLOWER.exception());
        }

        // 使用 computeIfAbsent 原子性地获取或创建
        return asyncLogs.computeIfAbsent(topicPartition, tp -> {
            CompletableFuture<BookkeeperUnifiedLog> future = new CompletableFuture<>();
            // 异步初始化
            initializeLogAsync(topicPartition, topicId, future);
            return future;
        });
    }

    // Original Scala lines 161-207: private def initializeLogAsync(...)
    private void initializeLogAsync(TopicPartition topicPartition,
                                    Optional<Uuid> topicId,
                                    CompletableFuture<BookkeeperUnifiedLog> future) {
        final BookkeeperStorage bookkeeperStorage = bookkeeperStorageOpt;
        final AsyncTransactionIndex transactionIndex;
        final AsyncProducerStateManager producerStateManager;
        final BookkeeperLocalLog localLog;
        try {
            transactionIndex = new AsyncTransactionIndex(bookkeeperStorage.getMetadataStoreExtended(), topicPartition);
            producerStateManager = new AsyncProducerStateManager(topicPartition, maxTransactionTimeoutMs(),
                producerStateManagerConfig(), time, bookkeeperStorage.getMetadataStoreExtended());
            localLog = new BookkeeperLocalLog(initialDefaultConfig(), scheduler, topicPartition, transactionIndex);
        } catch (IOException e) {
            LOG.error("Failed to initialize log for {}", topicPartition, e);
            asyncLogs.remove(topicPartition, future);
            future.completeExceptionally(e);
            return;
        }
        final AsyncProducerStateManager psm = producerStateManager;
        final BookkeeperLocalLog ll = localLog;

        ll.initializeAsync(bookkeeperStorage.getManagedLedgerFactory())
            .thenApply(startOffset -> {
                try {
                    BookkeeperUnifiedLog bookkeeperUnifiedLog = new BookkeeperUnifiedLog(
                        ll.logStartOffset(),
                        ll,
                        brokerTopicStats,
                        producerIdExpirationCheckIntervalMs(),
                        null,
                        psm,
                        topicId,
                        false,
                        LogOffsetsListener.NO_OP_OFFSETS_LISTENER
                    );
                    LOG.info("Initialized BookkeeperUnifiedLog for {} , logStartOffset: {}, logEndOffset: {}",
                        topicPartition, startOffset, ll.logEndOffset());
                    return bookkeeperUnifiedLog;
                } catch (Throwable t) {
                    LOG.error("Failed to initialize log for {}", topicPartition, t);
                    throw Errors.NOT_LEADER_OR_FOLLOWER.exception();
                }
            })
            .thenCompose(bookkeeperUnifiedLog -> bookkeeperUnifiedLog.initialize())
            .thenAccept(future::complete)
            .exceptionally(t -> {
                LOG.error("Failed to initialize log for {}", topicPartition, t);
                // 初始化失败时从 map 中移除，允许后续重试
                asyncLogs.remove(topicPartition, future);
                future.completeExceptionally(t);
                return null;
            });
    }

    // Original Scala lines 210-220: override def shutdown(brokerEpoch: Long)
    @Override
    public void shutdown(long brokerEpoch) {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        asyncLogs.values().forEach(logFuture ->
            logFuture.thenAccept(UnifiedLog::close)
        );
        asyncLogs.clear();
    }

    // Original Scala lines 222-226: override def directoryIdsSet
    @Override
    public Set<Uuid> directoryIdsSet() {
        Set<Uuid> set = new HashSet<>();
        set.add(new Uuid(brokerId, brokerId));
        return set;
    }

    // Original Scala lines 228-230: override def truncateTo(...) — noop, for ISR
    @Override
    public void truncateTo(Map<TopicPartition, Long> partitionOffsets, boolean isFuture) {
        // noop, for ISR
    }

    // Original Scala lines 232-237: override def truncateFullyAndStartAt(...) — noop, for ISR
    @Override
    public void truncateFullyAndStartAt(TopicPartition topicPartition,
                                        long newOffset,
                                        boolean isFuture,
                                        Optional<Long> logStartOffsetOpt) {
        // noop, for ISR
    }

    // Original Scala lines 239-241: override def checkpointLogRecoveryOffsets() — noop
    @Override
    public void checkpointLogRecoveryOffsets() {
        // noop
    }

    // Original Scala lines 243-245: override def checkpointLogStartOffsets() — noop
    @Override
    public void checkpointLogStartOffsets() {
        // noop
    }

    // Original Scala lines 247-249: override def maybeUpdatePreferredLogDir(...) — noop
    @Override
    public void maybeUpdatePreferredLogDir(TopicPartition topicPartition, String logDir) {
        // noop
    }

    // Original Scala lines 251-253: override def abortAndPauseCleaning(...) — noop
    @Override
    public void abortAndPauseCleaning(TopicPartition topicPartition) {
        // noop
    }

    // Original Scala lines 255-257: override def abortCleaning(...) — noop
    @Override
    public void abortCleaning(TopicPartition topicPartition) {
        // noop
    }

    // Original Scala lines 259-261: override def resumeCleaning(...) — noop
    @Override
    public void resumeCleaning(TopicPartition topicPartition) {
        // noop
    }

    // Original Scala lines 263-269: override def updateTopicConfig(...) — TODO
    @Override
    public void updateTopicConfig(String topic,
                                  Properties newTopicConfig,
                                  boolean isRemoteLogStorageSystemEnabled,
                                  boolean wasRemoteLogEnabled) {
        // TODO
        // 实现这个方法，更新Log Config
    }

    // Original Scala lines 271-273: override def topicConfigUpdated(...) — noop
    @Override
    public void topicConfigUpdated(String topic) {
        // noop
    }

    // Original Scala lines 275-278: override def brokerConfigUpdated() — TODO
    @Override
    public void brokerConfigUpdated() {
        // TODO
        // 或许应该实现
    }

    // Original Scala lines 280-282: override def initializingLog(...) — noop
    @Override
    public void initializingLog(TopicPartition topicPartition) {
        // noop
    }

    // Original Scala lines 284-286: override def finishedInitializingLog(...) — noop
    @Override
    public void finishedInitializingLog(TopicPartition topicPartition, Optional<UnifiedLog> maybeLog) {
        // noop
    }

    // Original Scala lines 288-290: override def replaceCurrentWithFutureLog(tp) — noop
    @Override
    public void replaceCurrentWithFutureLog(TopicPartition topicPartition) {
        // noop
    }

    // Original Scala lines 296-322: override def asyncDelete(tp, isFuture, checkpoint, isStray)
    @Override
    public Optional<UnifiedLog> asyncDelete(TopicPartition topicPartition,
                                            boolean isFuture,
                                            boolean checkpoint,
                                            boolean isStray) {
        CompletableFuture<BookkeeperUnifiedLog> logFuture = asyncLogs.remove(topicPartition);
        if (logFuture == null || !logFuture.isDone() || logFuture.isCompletedExceptionally()) {
            return Optional.empty();
        }
        BookkeeperUnifiedLog log = logFuture.join();
        log.close();

        String name = topicPartition.topic() + "-" + topicPartition.partition();
        if (bookkeeperStorageOpt != null) {
            bookkeeperStorageOpt.getManagedLedgerFactory().asyncDelete(name, new AsyncCallbacks.DeleteLedgerCallback() {
                @Override
                public void deleteLedgerComplete(Object ctx) {
                    LOG.info("Deleted log for {}", topicPartition);
                }

                @Override
                public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    LOG.error("Failed to delete log for {}", topicPartition, exception);
                }
            }, null);
        }

        return Optional.of(log);
    }

    // Original Scala lines 324-359: override def asyncDelete(iterable, isStray, errorHandler)
    // NOTE: Java parent signature uses Set<TopicPartition> + BiConsumer (not Iterable + Function2).
    @Override
    public void asyncDelete(Set<TopicPartition> topicPartitions,
                            boolean isStray,
                            BiConsumer<TopicPartition, Throwable> errorHandler) {
        CountDownLatch latch = new CountDownLatch(topicPartitions.size());
        for (TopicPartition topicPartition : topicPartitions) {
            CompletableFuture<BookkeeperUnifiedLog> logFuture = asyncLogs.remove(topicPartition);
            if (logFuture != null && logFuture.isDone() && !logFuture.isCompletedExceptionally()) {
                BookkeeperUnifiedLog log = logFuture.join();
                final TopicPartition tp = topicPartition;
                log.closeAsync().thenAccept(v -> {
                    String name = tp.topic() + "-" + tp.partition();
                    if (bookkeeperStorageOpt != null) {
                        bookkeeperStorageOpt.getManagedLedgerFactory().asyncDelete(name, new AsyncCallbacks.DeleteLedgerCallback() {
                            @Override
                            public void deleteLedgerComplete(Object ctx) {
                                LOG.info("Deleted log for {}", tp);
                                latch.countDown();
                            }

                            @Override
                            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                errorHandler.accept(tp, exception);
                                latch.countDown();
                            }
                        }, null);
                    }
                });
            } else {
                latch.countDown();
            }
        }

        try {
            latch.await();
        } catch (Throwable e) {
            LOG.error("Failed to delete logs for {}", topicPartitions, e);
        }
    }

    // Original Scala lines 361-367: override def allLogs
    @Override
    public Set<UnifiedLog> allLogs() {
        Set<UnifiedLog> logs = new HashSet<>();
        asyncLogs.values().forEach(f -> {
            if (f.isDone() && !f.isCompletedExceptionally()) {
                logs.add(f.join());
            }
        });
        return logs;
    }

    // Original Scala lines 369-380: override def logsByTopic(topic)
    @Override
    public List<UnifiedLog> logsByTopic(String topic) {
        List<UnifiedLog> logs = new ArrayList<>();
        asyncLogs.forEach((topicPartition, future) -> {
            if (Objects.equals(topicPartition.topic(), topic)) {
                if (future.isDone() && !future.isCompletedExceptionally()) {
                    logs.add(future.join());
                }
            }
        });
        return logs;
    }

    // Original Scala lines 382-384: override def isLogDirOnline(logDir) = true
    @Override
    public boolean isLogDirOnline(String logDir) {
        return true;
    }

    // Original Scala lines 386-388: override def readBrokerEpochFromCleanShutdownFiles()
    @Override
    public OptionalLong readBrokerEpochFromCleanShutdownFiles() {
        return OptionalLong.of(0L);
    }

    // Original Scala lines 390-392: override def liveLogDirs = Seq.empty
    @Override
    public Collection<File> liveLogDirs() {
        return new ArrayList<>();
    }

    // Original Scala lines 394-396: override def handleLogDirFailure(dir) — noop
    @Override
    public void handleLogDirFailure(String dir) {
        // noop
    }

    // Original Scala lines 398-400: override def resizeRecoveryThreadPool(newSize) — noop
    @Override
    public void resizeRecoveryThreadPool(int newSize) {
        // noop
    }

    /**
     * Original Scala companion object (lines 404-443): factory method.
     * Kept as a static factory in Java.
     */
    public static AsyncLogManager apply(KafkaConfig config,
                                        List<String> initialOfflineDirs,
                                        ConfigRepository configRepository,
                                        Scheduler kafkaScheduler,
                                        Time time,
                                        BrokerTopicStats brokerTopicStats,
                                        LogDirFailureChannel logDirFailureChannel) throws Exception {
        java.util.Map<String, Object> defaultProps = config.extractLogConfigMap();

        LogConfig.validateBrokerLogConfigValues(defaultProps, config.remoteLogManagerConfig().isRemoteStorageSystemEnabled());
        LogConfig defaultLogConfig = new LogConfig(defaultProps);

        CleanerConfig cleanerConfig = new CleanerConfig(config);
        TransactionLogConfig transactionLogConfig = new TransactionLogConfig(config);

        List<File> logDirs = config.logDirs().stream()
            .map(s -> new File(s).getAbsoluteFile())
            .collect(Collectors.toList());
        List<File> offlineDirs = initialOfflineDirs.stream()
            .map(s -> new File(s).getAbsoluteFile())
            .collect(Collectors.toList());

        return new AsyncLogManager(
            config.brokerId(),
            logDirs,
            offlineDirs,
            configRepository,
            defaultLogConfig,
            cleanerConfig,
            config.numRecoveryThreadsPerDataDir(),
            config.logFlushSchedulerIntervalMs(),
            config.logFlushOffsetCheckpointIntervalMs(),
            config.logFlushStartOffsetCheckpointIntervalMs(),
            config.logCleanupIntervalMs(),
            new TransactionStateManagerConfig(config).transactionMaxTimeoutMs(),
            new ProducerStateManagerConfig(transactionLogConfig.producerIdExpirationMs(), transactionLogConfig.transactionPartitionVerificationEnable()),
            transactionLogConfig.producerIdExpirationCheckIntervalMs(),
            kafkaScheduler,
            brokerTopicStats,
            logDirFailureChannel,
            time,
            config.remoteLogManagerConfig().isRemoteStorageSystemEnabled(),
            config.logInitialTaskDelayMs(),
            (cc, files, map, ldrc, t) -> new LogCleaner(cc, files, map, ldrc, t));
    }
}
