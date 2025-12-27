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
package org.apache.kafka.storage.internals.log.bookkeeper;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LocalLog;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A UnifiedLog implementation that uses Apache BookKeeper for data storage.
 *
 * <p>This class extends UnifiedLog to support asynchronous read/write operations
 * which are necessary for remote storage backends like BookKeeper. Since BookKeeper
 * is a distributed log storage system, all I/O operations involve network calls
 * and must be handled asynchronously.
 *
 * <p><b>Key differences from file-based UnifiedLog:</b>
 * <ul>
 *   <li>Uses BookKeeper's ManagedLedger for data storage instead of local files</li>
 *   <li>Implements truly async I/O operations via CompletableFuture</li>
 *   <li>BookKeeper handles replication internally, so ISR mechanism is simplified</li>
 *   <li>Offset and timestamp indexes are managed differently (not local files)</li>
 *   <li>Data is read as ByteBuf and wrapped in MemoryRecords</li>
 * </ul>
 *
 * <p><b>BookKeeper Integration Notes:</b>
 * <ul>
 *   <li>Each Kafka partition maps to a ManagedLedger</li>
 *   <li>Kafka offsets need to be mapped to BookKeeper (ledgerId, entryId)</li>
 *   <li>BookKeeper provides built-in replication through ensemble and quorum settings</li>
 * </ul>
 *
 * @see <a href="https://bookkeeper.apache.org/">Apache BookKeeper</a>
 * @see <a href="https://github.com/apache/pulsar/tree/master/managed-ledger">Pulsar ManagedLedger</a>
 * @see <a href="https://github.com/streamnative/kop">KoP (Kafka on Pulsar)</a>
 */
public class BookkeeperUnifiedLog extends UnifiedLog {

    private static final Logger LOG = LoggerFactory.getLogger(BookkeeperUnifiedLog.class);

    private final BookkeeperConfig bookkeeperConfig;
    
    // TODO: Add ManagedLedger when integrating with actual BookKeeper/Pulsar
    // private final ManagedLedger managedLedger;

    /**
     * Creates a new BookkeeperUnifiedLog instance.
     *
     * @param logStartOffset The earliest offset allowed to be exposed to kafka client
     * @param localLog The LocalLog instance (used for compatibility, may be minimal for BK)
     * @param brokerTopicStats Broker topic statistics
     * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
     * @param leaderEpochCache The LeaderEpochFileCache instance
     * @param producerStateManager Producer state manager for idempotent/transactional writes
     * @param topicId The topic ID
     * @param remoteStorageSystemEnable Whether remote storage is enabled
     * @param logOffsetsListener listener invoked when the high watermark is updated
     * @param bookkeeperConfig BookKeeper-specific configuration
     * @throws IOException if initialization fails
     */
    public BookkeeperUnifiedLog(long logStartOffset,
                                 LocalLog localLog,
                                 BrokerTopicStats brokerTopicStats,
                                 int producerIdExpirationCheckIntervalMs,
                                 LeaderEpochFileCache leaderEpochCache,
                                 ProducerStateManager producerStateManager,
                                 Optional<Uuid> topicId,
                                 boolean remoteStorageSystemEnable,
                                 LogOffsetsListener logOffsetsListener,
                                 BookkeeperConfig bookkeeperConfig) throws IOException {
        super(logStartOffset, localLog, brokerTopicStats, producerIdExpirationCheckIntervalMs,
              leaderEpochCache, producerStateManager, topicId, remoteStorageSystemEnable, logOffsetsListener);
        this.bookkeeperConfig = bookkeeperConfig;
        
        LOG.info("Created BookkeeperUnifiedLog for partition {} with config: {}",
                topicPartition(), bookkeeperConfig);
    }

    /**
     * Get the BookKeeper configuration for this log.
     *
     * @return The BookKeeper configuration
     */
    public BookkeeperConfig bookkeeperConfig() {
        return bookkeeperConfig;
    }

    /**
     * Check if this log uses BookKeeper-based storage.
     *
     * @return true since this is a BookKeeper-backed log
     */
    public boolean isBookkeeperBacked() {
        return true;
    }

    /**
     * Asynchronously append records to BookKeeper.
     *
     * <p>This method performs a truly asynchronous write to BookKeeper's ManagedLedger.
     * Since BookKeeper is a remote storage system, all writes involve network I/O
     * and are inherently asynchronous.
     *
     * <p><b>Implementation Notes:</b>
     * <ul>
     *   <li>Records are serialized to ByteBuf for BookKeeper</li>
     *   <li>ManagedLedger.asyncAddEntry() is used for async writes</li>
     *   <li>BookKeeper handles replication via quorum writes</li>
     *   <li>Offset assignment happens after successful BookKeeper write</li>
     * </ul>
     *
     * @param records The records to append
     * @param leaderEpoch The epoch of the replica appending
     * @param origin Declares the origin of the append which affects required validations
     * @param requestLocal request local instance
     * @param verificationGuard verification guard for transaction verification
     * @param transactionVersion the transaction version for the records
     * @return A CompletableFuture that completes with the LogAppendInfo when the append is done
     */
    @Override
    public CompletableFuture<LogAppendInfo> asyncAppend(
            MemoryRecords records,
            int leaderEpoch,
            AppendOrigin origin,
            RequestLocal requestLocal,
            VerificationGuard verificationGuard,
            short transactionVersion) {
        
        LOG.debug("Async append to BookKeeper for partition {}, records size: {}",
                topicPartition(), records.sizeInBytes());

        // TODO: Implement actual BookKeeper async write
        // Example implementation outline:
        // 
        // 1. Validate and prepare records (similar to sync append)
        // LogAppendInfo appendInfo = analyzeAndValidateRecords(records, origin, ...);
        //
        // 2. Convert MemoryRecords to ByteBuf for BookKeeper
        // ByteBuf byteBuf = Unpooled.wrappedBuffer(records.buffer());
        //
        // 3. Async write to ManagedLedger
        // CompletableFuture<Position> future = new CompletableFuture<>();
        // managedLedger.asyncAddEntry(byteBuf, new AddEntryCallback() {
        //     @Override
        //     public void addComplete(Position position, ByteBuf entryData, Object ctx) {
        //         // Update offset mapping
        //         // Update producer state
        //         // Complete the future with LogAppendInfo
        //         future.complete(position);
        //     }
        //
        //     @Override
        //     public void addFailed(ManagedLedgerException exception, Object ctx) {
        //         future.completeExceptionally(exception);
        //     }
        // }, null);
        //
        // return future.thenApply(position -> createLogAppendInfo(position, appendInfo));

        // For now, fall back to the default implementation
        // This should be replaced with actual BookKeeper integration
        return super.asyncAppend(records, leaderEpoch, origin, requestLocal, verificationGuard, transactionVersion);
    }

    /**
     * Asynchronously read records from BookKeeper.
     *
     * <p>This method performs a truly asynchronous read from BookKeeper's ManagedLedger.
     * Since BookKeeper is a remote storage system, all reads involve network I/O
     * and are inherently asynchronous.
     *
     * <p><b>Implementation Notes:</b>
     * <ul>
     *   <li>Kafka offset is mapped to BookKeeper (ledgerId, entryId)</li>
     *   <li>ManagedCursor.asyncReadEntries() is used for async reads</li>
     *   <li>Entries are converted from ByteBuf to MemoryRecords</li>
     *   <li>Multiple entries may be batched for efficiency</li>
     * </ul>
     *
     * @param startOffset The offset to begin reading at
     * @param maxLength The maximum number of bytes to read
     * @param isolation The fetch isolation, which controls the maximum offset we are allowed to read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds maxLength
     * @return A CompletableFuture that completes with the FetchDataInfo when the read is done
     */
    @Override
    public CompletableFuture<FetchDataInfo> asyncRead(
            long startOffset,
            int maxLength,
            FetchIsolation isolation,
            boolean minOneMessage) {
        
        LOG.debug("Async read from BookKeeper for partition {}, startOffset: {}, maxLength: {}",
                topicPartition(), startOffset, maxLength);

        // TODO: Implement actual BookKeeper async read
        // Example implementation outline:
        //
        // 1. Map Kafka offset to BookKeeper position
        // Position startPosition = offsetMapper.getPosition(startOffset);
        //
        // 2. Create or get ManagedCursor for reading
        // ManagedCursor cursor = managedLedger.openCursor("kafka-reader-" + topicPartition());
        //
        // 3. Async read entries
        // CompletableFuture<List<Entry>> future = new CompletableFuture<>();
        // cursor.asyncReadEntries(numEntries, new ReadEntriesCallback() {
        //     @Override
        //     public void readEntriesComplete(List<Entry> entries, Object ctx) {
        //         // Convert entries to MemoryRecords
        //         // Create FetchDataInfo
        //         future.complete(entries);
        //     }
        //
        //     @Override
        //     public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        //         future.completeExceptionally(exception);
        //     }
        // }, null, PositionFactory.create(startPosition.ledgerId(), startPosition.entryId()));
        //
        // return future.thenApply(entries -> createFetchDataInfo(entries, startOffset));

        // For now, fall back to the default implementation
        // This should be replaced with actual BookKeeper integration
        return super.asyncRead(startOffset, maxLength, isolation, minOneMessage);
    }

    /**
     * Since BookKeeper handles replication internally through quorum writes,
     * the traditional Kafka ISR (In-Sync Replicas) mechanism may not be needed.
     *
     * <p>BookKeeper provides:
     * <ul>
     *   <li>Ensemble size: Number of bookies to spread entries</li>
     *   <li>Write quorum: Number of bookies to write each entry</li>
     *   <li>Ack quorum: Number of acknowledgments required for write success</li>
     * </ul>
     *
     * <p>This method indicates whether ISR tracking should be enabled.
     * When using BookKeeper, ISR tracking can be disabled as BookKeeper
     * guarantees durability through its own replication mechanism.
     *
     * @return false to indicate ISR tracking is not needed with BookKeeper
     */
    public boolean isIsrTrackingEnabled() {
        // BookKeeper handles replication internally, so Kafka's ISR mechanism
        // is not strictly necessary. However, this may need coordination with
        // the controller and other components.
        return !bookkeeperConfig.disableIsrTracking();
    }

    /**
     * Close this BookKeeper-backed log and release associated resources.
     */
    @Override
    public void close() {
        LOG.info("Closing BookkeeperUnifiedLog for partition {}", topicPartition());
        
        // TODO: Close ManagedLedger when integrated
        // if (managedLedger != null) {
        //     managedLedger.close();
        // }
        
        super.close();
    }
}
