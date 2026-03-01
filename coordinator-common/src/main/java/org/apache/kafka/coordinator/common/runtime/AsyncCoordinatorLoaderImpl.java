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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AsyncCoordinatorLoaderImpl<T> extends CoordinatorLoaderImpl<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncCoordinatorLoaderImpl.class);

    public AsyncCoordinatorLoaderImpl(Time time,
                                      Function<TopicPartition, Optional<UnifiedLog>> partitionLogSupplier,
                                      Function<TopicPartition, Optional<Long>> partitionLogEndOffsetSupplier,
                                      Deserializer<T> deserializer, int loadBufferSize, long commitIntervalOffsets) {
        super(time, partitionLogSupplier, partitionLogEndOffsetSupplier, deserializer, loadBufferSize, commitIntervalOffsets);
    }


    @Override
    protected void doLoad(TopicPartition tp,
                          CoordinatorPlayback<T> coordinator,
                          CompletableFuture<LoadSummary> future,
                          long startTimeMs) {
        long schedulerQueueTimeMs = time.milliseconds() - startTimeMs;
        try {
            Optional<UnifiedLog> logOpt = partitionLogSupplier.apply(tp);
            if (logOpt.isEmpty()) {
                future.completeExceptionally(new NotLeaderOrFollowerException(
                        "Could not load records from " + tp + " because the log does not exist."));
                return;
            }

            UnifiedLog log = logOpt.get();
            if (!log.config().asyncLogModeEnable) {
                super.doLoad(tp, coordinator, future, startTimeMs);
                return;
            }

            // Start async loading with initial state
            AsyncLoadContext context = new AsyncLoadContext(
                log,
                ByteBuffer.allocate(0),
                log.logStartOffset(),
                -1L,
                new LoadStats(),
                schedulerQueueTimeMs
            );

            // The loadNextBatchAsync will complete the future
            loadNextBatchAsync(tp, coordinator, future, startTimeMs, context);
        } catch (Throwable ex) {
            // Only complete if not already done
            if (!future.isDone()) {
                LOG.error("Error starting async load from {}", tp, ex);
                future.completeExceptionally(ex);
            }
        }
    }

    /**
     * Recursively loads batches of records asynchronously until all records are loaded.
     */
    private CompletableFuture<LoadSummary> loadNextBatchAsync(
        TopicPartition tp,
        CoordinatorPlayback<T> coordinator,
        CompletableFuture<LoadSummary> future,
        long startTimeMs,
        AsyncLoadContext context
    ) {
        // Early exit if loader was stopped
        if (!isRunning.get()) {
            completeFutureIfNeeded(future, new RuntimeException("Coordinator loader is closed."));
            return CompletableFuture.failedFuture(new RuntimeException("Coordinator loader is closed."));
        }

        // Check if we should continue loading
        if (!shouldFetchNextBatch(context.currentOffset, logEndOffset(tp), context.stats.readAtLeastOneRecord)) {
            long endTimeMs = time.milliseconds();

            if (logEndOffset(tp) == -1L) {
                completeFutureIfNeeded(future, new NotLeaderOrFollowerException(
                        String.format("Stopped loading records from %s because the partition is not online or is no longer the leader.", tp)));
                return CompletableFuture.failedFuture(new NotLeaderOrFollowerException(
                        String.format("Stopped loading records from %s because the partition is not online or is no longer the leader.", tp)));
            } else {
                LoadSummary summary = new LoadSummary(startTimeMs, endTimeMs, context.schedulerQueueTimeMs,
                    context.stats.numRecords, context.stats.numBytes);
                completeFutureIfNeeded(future, summary);
                return CompletableFuture.completedFuture(summary);
            }
        }

        // Async read the next batch
        return context.log.readAsync(context.currentOffset, loadBufferSize, FetchIsolation.LOG_END, true)
            .thenApply(fetchDataInfo -> {
                // Immediately extract the records to avoid holding FetchDataInfo longer than needed
                // This allows FetchDataInfo to be GC'd sooner
                return fetchDataInfo.records;
            })
            .thenCompose(records -> {
                try {
                    context.stats.readAtLeastOneRecord = records.sizeInBytes() > 0;

                    // Convert to readable MemoryRecords and reuse buffer if needed
                    MemoryRecords memoryRecords = toReadableMemoryRecords(tp, records, context.buffer);

                    // Update context.buffer when we allocated a new buffer (FileRecords case)
                    // memoryRecords.buffer() returns a duplicate, but it shares the same underlying array
                    // which is what we need for reuse in the next iteration
                    if (records instanceof FileRecords) {
                        context.buffer = memoryRecords.buffer();
                    }

                    // Process the batch
                    ReplayResult replayResult = processMemoryRecords(
                        tp, context.log, memoryRecords, coordinator,
                        context.stats, context.currentOffset, context.lastCommittedOffset
                    );

                    // Update context for next iteration
                    context.currentOffset = replayResult.nextOffset();
                    context.lastCommittedOffset = replayResult.lastCommittedOffset();

                    // Recursively load next batch
                    return loadNextBatchAsync(tp, coordinator, future, startTimeMs, context);

                } catch (IOException ex) {
                    // Convert checked IOException to RuntimeException
                    KafkaStorageException runtimeEx = new KafkaStorageException(
                        String.format("I/O error while loading records from %s at offset %d",
                            tp, context.currentOffset), ex);
                    LOG.error("I/O error loading records from {} at offset {}", tp, context.currentOffset, ex);
                    completeFutureIfNeeded(future, runtimeEx);
                    return CompletableFuture.failedFuture(runtimeEx);
                } catch (Throwable ex) {
                    // Log and propagate any other exceptions
                    LOG.error("Unexpected error loading records from {} at offset {}", tp, context.currentOffset, ex);
                    completeFutureIfNeeded(future, ex);
                    return CompletableFuture.failedFuture(ex);
                }
            })
            .exceptionally(ex -> {
                // Handle exceptions from readAsync itself
                if (!future.isDone()) {
                    LOG.error("Async read failed from {} at offset {}", tp, context.currentOffset, ex);
                    completeFutureIfNeeded(future, ex);
                }
                return null;
            });
    }

    /**
     * Completes the future if it's not already completed.
     * This prevents duplicate completion exceptions.
     */
    private void completeFutureIfNeeded(CompletableFuture<LoadSummary> future, LoadSummary result) {
        if (!future.isDone()) {
            future.complete(result);
        }
    }

    /**
     * Completes the future exceptionally if it's not already completed.
     * This prevents duplicate completion exceptions.
     */
    private void completeFutureIfNeeded(CompletableFuture<LoadSummary> future, Throwable ex) {
        if (!future.isDone()) {
            future.completeExceptionally(ex);
        }
    }

    /**
     * Context class to hold state during async loading.
     */
    private static class AsyncLoadContext {
        final UnifiedLog log;
        ByteBuffer buffer;
        long currentOffset;
        long lastCommittedOffset;
        LoadStats stats;
        final long schedulerQueueTimeMs;

        AsyncLoadContext(
            UnifiedLog log,
            ByteBuffer buffer,
            long currentOffset,
            long lastCommittedOffset,
            LoadStats stats,
            long schedulerQueueTimeMs
        ) {
            this.log = log;
            this.buffer = buffer;
            this.currentOffset = currentOffset;
            this.lastCommittedOffset = lastCommittedOffset;
            this.stats = stats;
            this.schedulerQueueTimeMs = schedulerQueueTimeMs;
        }
    }
}
