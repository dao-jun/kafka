# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Async PartitionWriter Implementation Design

## Document Information

- **Date**: 2026-02-07
- **Author**: Design Session
- **Status**: Draft
- **Target Version**: TBD

## Executive Summary

This document describes the design for converting the synchronous `PartitionWriter.append()` calls in `CoordinatorRuntime` to fully asynchronous `appendAsync()` calls. The goal is to achieve end-to-end asynchronous write operations while maintaining strict ordering guarantees and READ COMMITTED semantics.

### Key Design Decisions

1. **READ COMMITTED Semantics**: Data is only applied to the state machine AFTER `appendAsync` succeeds
2. **Strict Sequential Ordering**: Only one batch can be in-flight at a time
3. **Self-Managed HWM**: High watermark updates are triggered manually after state machine updates, not via `PartitionWriter.Listener`
4. **Unified Write-Then-Replay**: Both regular batches and transaction markers follow "write first, replay after success" pattern

---

## Table of Contents

1. [Background and Motivation](#background-and-motivation)
2. [Design Principles](#design-principles)
3. [Architecture Overview](#architecture-overview)
4. [Data Structure Changes](#data-structure-changes)
5. [Core Flow Changes](#core-flow-changes)
6. [Error Handling](#error-handling)
7. [Transaction Support](#transaction-support)
8. [State Management](#state-management)
9. [Implementation Plan](#implementation-plan)
10. [Testing Strategy](#testing-strategy)
11. [Risks and Mitigations](#risks-and-mitigations)

---

## Background and Motivation

### Current Implementation

Currently, `CoordinatorRuntime` uses synchronous `PartitionWriter.append()` for writing records:

1. Allocate offsets for records
2. Immediately replay records to state machine (`coordinator.replay()`)
3. Add records to batch builder
4. Flush batch synchronously (`partitionWriter.append()`)
5. If flush fails, revert state machine changes

**Problems with Current Approach**:
- Blocking I/O operations block the event queue
- State machine updates before persistence (not READ COMMITTED)
- Complex revert logic on failure
- Lower throughput due to blocking

### New appendAsync Method

A new `appendAsync()` method has been added to `PartitionWriter`:

```java
default CompletableFuture<Long> appendAsync(
    TopicPartition tp,
    VerificationGuard verificationGuard,
    MemoryRecords records,
    short transactionVersion
) {
    try {
        return CompletableFuture.completedFuture(
            append(tp, verificationGuard, records, transactionVersion)
        );
    } catch (Throwable t) {
        return CompletableFuture.failedFuture(t);
    }
}
```

**Future Implementation**: This method will write to remote storage (multi-replica storage component) and may trigger `HighWatermarkListener.onHighWatermarkUpdated` before the callback completes.

### Goals

1. **Non-blocking**: Write operations should not block the event queue
2. **READ COMMITTED**: Reads should only see data that has been successfully persisted
3. **Strict Ordering**: Maintain the same ordering guarantees as the current implementation
4. **No Data Loss**: Handle all failure scenarios correctly
5. **Production Ready**: Fully tested and ready for production use

---

## Design Principles

### 1. READ COMMITTED Semantics

**Principle**: Data is only visible to readers AFTER it has been successfully persisted.

**Implementation**:
- Records are NOT replayed to the state machine during `append()`
- Records are stored in `CoordinatorBatch.recordsToReplay` list
- Replay happens in `handleAsyncFlushCompletion()` AFTER `appendAsync` succeeds
- If `appendAsync` fails, state machine is never updated (no revert needed)

### 2. Strict Sequential Ordering

**Principle**: Only one batch can be in-flight at a time to ensure strict ordering.

**Implementation**:
- `asyncOperationInProgress` flag prevents concurrent async operations
- New batch cannot be allocated while `asyncOperationInProgress == true`
- New batch cannot be flushed while `inFlightBatch != null`
- Offset allocation happens during batch creation (when previous batch is complete)

### 3. Self-Managed High Watermark

**Principle**: HWM updates are controlled by CoordinatorRuntime, not by external listeners.

**Implementation**:
- Do NOT register `HighWatermarkListener` via `PartitionWriter.registerListener()`
- Manually trigger HWM update in `handleAsyncFlushCompletion()` after state machine update
- Since `appendAsync` success implies HWM update (multi-replica storage), we can safely update immediately

**Rationale**:
- Avoids race condition where HWM listener fires before state machine update
- Ensures HWM always points to data that is visible in state machine
- Simpler control flow

### 4. Unified Write-Then-Replay Pattern

**Principle**: All writes (regular batches and transaction markers) follow the same pattern.

**Implementation**:
- Regular batches: Write via `appendAsync`, replay on success
- Transaction markers: Write via `appendAsync`, replay on success (changed from current)
- No revert logic needed (state machine not updated on failure)
- Consistent error handling

**Current vs New for Transaction Markers**:

Current (synchronous):
```
1. flushCurrentBatch()
2. coordinator.replayEndTransactionMarker()  // Update state first
3. partitionWriter.append(marker)            // Then write
4. If step 3 fails -> revert step 2
```

New (asynchronous):
```
1. flushCurrentBatch() -> wait for completion
2. partitionWriter.appendAsync(marker)       // Write first
3. On success -> coordinator.replayEndTransactionMarker()  // Then update state
4. If step 2 fails -> no revert needed
```

---

## Architecture Overview

### High-Level Flow

```
┌─────────────────┐
│  User Request   │
└────────┬────────┘
         │
         v
┌─────────────────┐
│   append()      │  - Check asyncOperationInProgress
│                 │  - Allocate batch if needed
│                 │  - Assign offsets
│                 │  - Store in recordsToReplay (NOT replay yet)
│                 │  - Add to builder
└────────┬────────┘
         │
         v
┌─────────────────┐
│ flushCurrentBatch() │  - Set asyncOperationInProgress = true
│                     │  - Move currentBatch to inFlightBatch
│                     │  - Call appendAsync()
└────────┬────────────┘
         │
         v
┌─────────────────────────┐
│  appendAsync()          │  - Write to remote storage
│  (in background)        │  - May trigger HWM listener
└────────┬────────────────┘
         │
         v
┌──────────────────────────┐
│ handleAsyncFlushCompletion() │  - Verify offset
│                              │  - Replay to state machine
│                              │  - Update lastWrittenOffset
│                              │  - Update lastCommittedOffset (HWM)
│                              │  - Complete deferred events
│                              │  - Set asyncOperationInProgress = false
└──────────────────────────────┘
```

### Key Components

1. **CoordinatorBatch**: Enhanced to store `recordsToReplay` list
2. **InFlightBatch**: Reference to the batch currently being written asynchronously
3. **asyncOperationInProgress**: Flag to prevent concurrent async operations
4. **handleAsyncFlushCompletion()**: New method to process async write results
5. **handleTransactionMarkerCompletion()**: New method to process transaction marker results

---

## Data Structure Changes

### 1. RecordToReplay Class (New)

```java
private static class RecordToReplay<U> {
    final long offset;
    final long producerId;
    final short producerEpoch;
    final U record;
    final SimpleRecord simpleRecord;
    final boolean shouldReplay;

    RecordToReplay(
        long offset,
        long producerId,
        short producerEpoch,
        U record,
        SimpleRecord simpleRecord,
        boolean shouldReplay
    ) {
        this.offset = offset;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.record = record;
        this.simpleRecord = simpleRecord;
        this.shouldReplay = shouldReplay;
    }
}
```

**Purpose**: Store all information needed to replay a record to the state machine after async write succeeds.

### 2. CoordinatorBatch Enhancement

```java
private static class CoordinatorBatch {
    // Existing fields
    final long baseOffset;
    final long appendTimeMs;
    final VerificationGuard verificationGuard;
    final ByteBuffer buffer;
    final MemoryRecordsBuilder builder;
    final Optional<TimerTask> lingerTimeoutTask;
    final DeferredEventCollection deferredEvents;
    long nextOffset;

    // New fields
    final long producerId;
    final short producerEpoch;
    final List<RecordToReplay<?>> recordsToReplay;  // NEW

    // Constructor updated to include new fields
}
```

**Changes**:
- Added `recordsToReplay` list to store records before replay
- Added `producerId` and `producerEpoch` for transaction support

### 3. CoordinatorContext Enhancement

```java
class CoordinatorContext {
    // Existing fields...

    // New fields
    private CoordinatorBatch inFlightBatch = null;
    private boolean asyncOperationInProgress = false;
}
```

**Purpose**:
- `inFlightBatch`: Track the batch currently being written asynchronously
- `asyncOperationInProgress`: Prevent concurrent async operations

---

## Core Flow Changes

### 1. maybeAllocateNewBatch() - Modified

**Current Behavior**: Allocate new batch when needed

**New Behavior**: Check for in-flight async operations before allocating

```java
private void maybeAllocateNewBatch(
    long producerId,
    short producerEpoch,
    VerificationGuard verificationGuard,
    long currentTimeMs
) {
    if (currentBatch == null) {
        // NEW: Check for in-flight async operations
        if (asyncOperationInProgress || inFlightBatch != null) {
            throw new IllegalStateException(
                "Cannot allocate new batch while async operation in progress for " + tp
            );
        }

        // Safe to allocate baseOffset (previous batch is complete)
        long baseOffset = coordinator.lastWrittenOffset() + 1;

        // Create new batch with recordsToReplay list
        currentBatch = new CoordinatorBatch(
            log,
            baseOffset,
            currentTimeMs,
            verificationGuard,
            producerId,
            producerEpoch,
            new ArrayList<>(), // recordsToReplay
            // ... other parameters
        );
    }
}
```

**Key Points**:
- Offset allocation happens here (when previous batch is complete)
- Cannot allocate if async operation in progress
- This ensures strict sequential ordering

- This ensures strict sequential ordering

### 2. append() - Modified

**Current Behavior**: Allocate offsets, replay immediately, add to builder

**New Behavior**: Allocate offsets, store for later replay, add to builder

```java
private void append(
    long producerId,
    short producerEpoch,
    VerificationGuard verificationGuard,
    List<U> records,
    boolean replay,
    boolean isAtomic,
    DeferredEvent event
) {
    // Check state
    if (state != CoordinatorState.ACTIVE) {
        throw new IllegalStateException("Coordinator must be active to append records");
    }

    if (records.isEmpty()) {
        waitForPendingWrites(event);
        return;
    }

    // NEW: Check for in-flight async operations
    if (asyncOperationInProgress) {
        waitForPendingWrites(event);
        return;
    }

    long currentTimeMs = time.milliseconds();

    // Handle transactional writes
    if (producerId != RecordBatch.NO_PRODUCER_ID) {
        isAtomic = true;
        flushCurrentBatch();
        // Check again after flush
        if (asyncOperationInProgress) {
            waitForPendingWrites(event);
            return;
        }
    }

    // Allocate new batch
    maybeAllocateNewBatch(producerId, producerEpoch, verificationGuard, currentTimeMs);

    // Prepare records
    for (U record : records) {
        SimpleRecord simpleRecord = new SimpleRecord(
            currentTimeMs,
            serializer.serializeKey(record),
            serializer.serializeValue(record)
        );

        // NEW: Allocate offset and store for later replay
        long recordOffset = currentBatch.nextOffset;
        currentBatch.recordsToReplay.add(new RecordToReplay<>(
            recordOffset,
            producerId,
            producerEpoch,
            record,
            simpleRecord,
            replay  // shouldReplay flag
        ));

        // Add to builder (but NOT replay yet)
        currentBatch.builder.append(simpleRecord);
        currentBatch.nextOffset++;
    }

    // Add event to batch
    currentBatch.deferredEvents.add(event);

    // Maybe flush batch
    maybeFlushCurrentBatch(currentTimeMs);
}
```

**Key Changes**:
- Check `asyncOperationInProgress` before proceeding
- Allocate offsets during append (not during replay)
- Store records in `recordsToReplay` list
- Do NOT call `coordinator.replay()` here
- Replay happens later in `handleAsyncFlushCompletion()`

### 3. flushCurrentBatch() - Modified

**Current Behavior**: Synchronously write batch via `partitionWriter.append()`

**New Behavior**: Asynchronously write batch via `partitionWriter.appendAsync()`

```java
private void flushCurrentBatch() {
    if (currentBatch != null) {
        try {
            // Check for empty batch
            if (currentBatch.builder.numRecords() == 0) {
                log.debug("Tried to flush an empty batch for {}.", tp);
                failCurrentBatch(new IllegalStateException("Record batch was empty"));
                return;
            }

            // Record metrics
            long flushStartMs = time.milliseconds();
            runtimeMetrics.recordLingerTime(flushStartMs - currentBatch.appendTimeMs);

            // NEW: Save reference to batch being flushed
            CoordinatorBatch batchToFlush = currentBatch;

            // NEW: Mark async operation in progress
            asyncOperationInProgress = true;
            inFlightBatch = batchToFlush;
            currentBatch = null;  // Release currentBatch reference

            // NEW: Async write
            CompletableFuture<Long> appendFuture = partitionWriter.appendAsync(
                tp,
                batchToFlush.verificationGuard,
                batchToFlush.builder.build(),
                TransactionVersion.TV_UNKNOWN
            );

            runtimeMetrics.recordFlushTime(time.milliseconds() - flushStartMs);

            // NEW: Handle async result
            appendFuture.whenComplete((offset, throwable) -> {
                // Enqueue completion event to event queue
                enqueueLast(new CoordinatorInternalEvent(
                    "AsyncFlushCompletion",
                    tp,
                    () -> handleAsyncFlushCompletion(batchToFlush, offset, throwable)
                ));
            });

        } catch (Throwable t) {
            // Synchronous exception (calling appendAsync itself failed)
            log.error("Starting async write to {} failed due to: {}.", tp, t.getMessage(), t);
            failCurrentBatch(t);
            throw t;
        }
    }
}
```

**Key Changes**:
- Set `asyncOperationInProgress = true` before async write
- Move `currentBatch` to `inFlightBatch`
- Release `currentBatch` reference (allows new batch creation, but blocked by `asyncOperationInProgress`)
- Use `enqueueLast()` to process result in event queue (thread-safe)

### 4. handleAsyncFlushCompletion() - New Method

**Purpose**: Process the result of async write operation

```java
private void handleAsyncFlushCompletion(
    CoordinatorBatch batch,
    Long offset,
    Throwable throwable
) {
    // This method runs in event queue (thread-safe)

    if (throwable != null) {
        // Async write failed
        log.error("Async write to {} failed due to: {}.", tp, throwable.getMessage(), throwable);

        // Fail all deferred events
        batch.deferredEvents.complete(throwable);

        // Clean up resources
        freeInFlightBatch();
        asyncOperationInProgress = false;

        // Transition to FAILED if needed
        if (shouldTransitionToFailed(throwable)) {
            transitionTo(CoordinatorState.FAILED);
            transitionTo(CoordinatorState.LOADING);
        }

        return;
    }

    // Async write succeeded
    log.debug("Async write to {} completed successfully with offset {}.", tp, offset);

    // Verify offset matches expectation
    if (offset != batch.nextOffset) {
        log.error("The state machine of the coordinator {} is out of sync with the underlying log. " +
            "The last written offset returned is {} while the coordinator expected {}. The coordinator " +
            "will be reloaded in order to re-synchronize the state machine.",
            tp, offset, batch.nextOffset);

        // Fail events
        batch.deferredEvents.complete(Errors.NOT_COORDINATOR.exception());

        // Clean up
        freeInFlightBatch();
        asyncOperationInProgress = false;

        // Trigger reload
        transitionTo(CoordinatorState.FAILED);
        transitionTo(CoordinatorState.LOADING);

        return;
    }

    // Check coordinator state
    if (state != CoordinatorState.ACTIVE) {
        log.warn("Coordinator {} is no longer active (state={}). Discarding async flush result.",
            tp, state);
        batch.deferredEvents.complete(Errors.NOT_COORDINATOR.exception());
        freeInFlightBatch();
        asyncOperationInProgress = false;
        return;
    }

    try {
        // NOW replay records to state machine
        for (RecordToReplay<U> recordToReplay : batch.recordsToReplay) {
            if (recordToReplay.shouldReplay) {
                coordinator.replay(
                    recordToReplay.offset,
                    recordToReplay.producerId,
                    recordToReplay.producerEpoch,
                    recordToReplay.record
                );
            }
        }

        // Update lastWrittenOffset
        coordinator.updateLastWrittenOffset(offset);

        // Self-trigger HWM update (appendAsync success implies HWM update)
        log.debug("Updating high watermark of {} to {} after async flush.", tp, offset);
        coordinator.updateLastCommittedOffset(offset);
        coordinatorMetrics.onUpdateLastCommittedOffset(tp, offset);

        // Complete deferred events
        deferredEventQueue.completeUpTo(offset);
        deferredEventQueue.add(offset, batch.deferredEvents);

    } catch (Throwable t) {
        // Replay failed (very rare)
        log.error("Replaying records to {} failed due to: {}.", tp, t.getMessage(), t);

        batch.deferredEvents.complete(t);
        freeInFlightBatch();
        asyncOperationInProgress = false;

        transitionTo(CoordinatorState.FAILED);
        transitionTo(CoordinatorState.LOADING);

        return;
    }

    // Success - clean up
    freeInFlightBatch();
    asyncOperationInProgress = false;

    // Flush next batch if exists
    if (currentBatch != null) {
        maybeFlushCurrentBatch(time.milliseconds());
    }
}
```

**Key Points**:
- Runs in event queue (thread-safe)
- Replay happens AFTER write succeeds
- Self-trigger HWM update (no external listener)
- Clean up `inFlightBatch` and reset `asyncOperationInProgress`
- Trigger next batch flush if available

### 5. waitForPendingWrites() - Modified

**Current Behavior**: Wait for currentBatch to complete

**New Behavior**: Wait for both inFlightBatch and currentBatch

```java
private void waitForPendingWrites(DeferredEvent event) {
    // NEW: Check inFlightBatch first (async operation in progress)
    if (inFlightBatch != null) {
        inFlightBatch.deferredEvents.add(event);
    } else if (currentBatch != null && currentBatch.builder.numRecords() > 0) {
        // Check currentBatch (not yet flushed)
        currentBatch.deferredEvents.add(event);
    } else {
        // Check for uncommitted data
        if (coordinator.lastCommittedOffset() < coordinator.lastWrittenOffset()) {
            deferredEventQueue.add(coordinator.lastWrittenOffset(),
                DeferredEventCollection.of(log, event));
        } else {
            event.complete(null);
        }
    }
}
```

**Key Change**: Check `inFlightBatch` first before `currentBatch`

---

## Error Handling

### Error Scenarios and Handling

#### 1. appendAsync Fails

**Scenario**: `partitionWriter.appendAsync()` returns a failed future

**Handling**:
```
1. handleAsyncFlushCompletion() receives throwable
2. Fail all deferred events in batch
3. Free inFlightBatch resources
4. Set asyncOperationInProgress = false
5. If serious error -> transition to FAILED -> LOADING
```

**Key Point**: No revert needed (state machine not updated yet)

#### 2. Offset Mismatch

**Scenario**: Returned offset != batch.nextOffset

**Handling**:
```
1. Log error about state machine out of sync
2. Fail all deferred events
3. Free inFlightBatch resources
4. Set asyncOperationInProgress = false
5. Transition to FAILED -> LOADING (triggers reload)
```

**Key Point**: State machine reload will resync from log

#### 3. Replay Fails

**Scenario**: `coordinator.replay()` throws exception

**Handling**:
```
1. Catch exception in handleAsyncFlushCompletion()
2. Fail all deferred events
3. Free inFlightBatch resources
4. Set asyncOperationInProgress = false
5. Transition to FAILED -> LOADING
```

**Key Point**: Very rare, indicates bug in coordinator logic

#### 4. State Not Active

**Scenario**: Coordinator state changed during async operation

**Handling**:
```
1. Check state in handleAsyncFlushCompletion()
2. If not ACTIVE -> discard result
3. Fail deferred events with NOT_COORDINATOR
4. Free inFlightBatch resources
5. Set asyncOperationInProgress = false
```

**Key Point**: State transition already handled cleanup

#### 5. Synchronous Exception

**Scenario**: Calling `appendAsync()` itself throws exception

**Handling**:
```
1. Catch in flushCurrentBatch()
2. Call failCurrentBatch(t)
3. Rethrow exception
```

**Key Point**: Same as current behavior

### Helper Methods

#### freeInFlightBatch()

```java
private void freeInFlightBatch() {
    if (inFlightBatch != null) {
        // Cancel linger timeout
        inFlightBatch.lingerTimeoutTask.ifPresent(TimerTask::cancel);

        // Release buffer
        int cachedBufferMaxBytes = cachedBufferMaxBytesSupplier.get();
        if (inFlightBatch.builder.buffer().capacity() <= cachedBufferMaxBytes) {
            bufferSupplier.release(inFlightBatch.builder.buffer());
            cachedBufferSize.set(inFlightBatch.builder.buffer().capacity());
        } else if (inFlightBatch.buffer.capacity() <= cachedBufferMaxBytes) {
            bufferSupplier.release(inFlightBatch.buffer);
            cachedBufferSize.set(inFlightBatch.buffer.capacity());
            runtimeMetrics.recordBufferCacheDiscarded();
        } else {
            runtimeMetrics.recordBufferCacheDiscarded();
            cachedBufferSize.set(0L);
        }

        inFlightBatch = null;
    }
}
```

#### shouldTransitionToFailed()

```java
private boolean shouldTransitionToFailed(Throwable t) {
    // Determine if error is serious enough to require reload
    return t instanceof KafkaException &&
           !(t instanceof TimeoutException);
}
```

---

## Transaction Support

### Transaction Marker Handling

**Current Approach** (synchronous):
```
1. flushCurrentBatch() - sync
2. coordinator.replayEndTransactionMarker() - update state first
3. partitionWriter.append(marker) - sync write
4. If step 3 fails -> revert step 2
```

**New Approach** (asynchronous, write-then-replay):
```
1. flushCurrentBatch() - async, wait for completion
2. partitionWriter.appendAsync(marker) - async write
3. On success -> coordinator.replayEndTransactionMarker() - update state after
4. If step 2 fails -> no revert needed
```

### completeTransaction() - Modified

```java
private void completeTransaction(
    long producerId,
    short producerEpoch,
    int coordinatorEpoch,
    TransactionResult result,
    short transactionVersion,
    DeferredEvent event
) {
    if (state != CoordinatorState.ACTIVE) {
        throw new IllegalStateException("Coordinator must be active to complete a transaction");
    }

    // Check for in-flight async operations
    if (asyncOperationInProgress) {
        waitForPendingWrites(event);
        return;
    }

    // Flush current batch first (ensure ordering)
    flushCurrentBatch();

    // Check again after flush
    if (asyncOperationInProgress) {
        // Flush triggered async operation, need to wait
        waitForPendingWrites(new DeferredEvent(log, event.context()) {
            @Override
            public void complete(Throwable exception) {
                if (exception != null) {
                    event.complete(exception);
                } else {
                    // Retry complete transaction
                    completeTransaction(producerId, producerEpoch, coordinatorEpoch,
                        result, transactionVersion, event);
                }
            }
        });
        return;
    }

    try {
        // Mark async operation in progress
        asyncOperationInProgress = true;

        // Build transaction marker
        MemoryRecords transactionMarker = MemoryRecords.withEndTransactionMarker(
            time.milliseconds(),
            producerId,
            producerEpoch,
            new EndTransactionMarker(
                result == TransactionResult.COMMIT ? ControlRecordType.COMMIT : ControlRecordType.ABORT,
                coordinatorEpoch
            )
        );

        long flushStartMs = time.milliseconds();

        // Async write (NOTE: NOT replaying first)
        CompletableFuture<Long> appendFuture = partitionWriter.appendAsync(
            tp,
            VerificationGuard.SENTINEL,
            transactionMarker,
            transactionVersion
        );

        runtimeMetrics.recordFlushTime(time.milliseconds() - flushStartMs);

        // Handle async result
        appendFuture.whenComplete((offset, throwable) -> {
            enqueueLast(new CoordinatorInternalEvent(
                "TransactionMarkerCompletion",
                tp,
                () -> handleTransactionMarkerCompletion(
                    producerId, producerEpoch, result, offset, throwable, event
                )
            ));
        });

    } catch (Throwable t) {
        // Synchronous exception
        log.error("Starting async write of transaction marker to {} failed due to: {}.",
            tp, t.getMessage(), t);
        asyncOperationInProgress = false;
        event.complete(t);
        throw t;
    }
}
```

### handleTransactionMarkerCompletion() - New Method

```java
private void handleTransactionMarkerCompletion(
    long producerId,
    short producerEpoch,
    TransactionResult result,
    Long offset,
    Throwable throwable,
    DeferredEvent event
) {
    if (throwable != null) {
        // Transaction marker write failed
        log.error("Async write of transaction marker to {} failed due to: {}.",
            tp, throwable.getMessage(), throwable);

        asyncOperationInProgress = false;
        event.complete(throwable);

        if (shouldTransitionToFailed(throwable)) {
            transitionTo(CoordinatorState.FAILED);
            transitionTo(CoordinatorState.LOADING);
        }

        return;
    }

    // Transaction marker write succeeded
    log.debug("Transaction marker written to {} at offset {}.", tp, offset);

    // Check coordinator state
    if (state != CoordinatorState.ACTIVE) {
        log.warn("Coordinator {} is no longer active (state={}). Discarding transaction marker result.",
            tp, state);
        asyncOperationInProgress = false;
        event.complete(Errors.NOT_COORDINATOR.exception());
        return;
    }

    try {
        // NOW replay transaction marker to state machine
        coordinator.replayEndTransactionMarker(
            producerId,
            producerEpoch,
            result
        );

        // Update lastWrittenOffset
        coordinator.updateLastWrittenOffset(offset);

        // Trigger HWM update
        log.debug("Updating high watermark of {} to {} after transaction marker.", tp, offset);
        coordinator.updateLastCommittedOffset(offset);
        coordinatorMetrics.onUpdateLastCommittedOffset(tp, offset);

        // Complete deferred events
        deferredEventQueue.add(offset, DeferredEventCollection.of(log, event));
        deferredEventQueue.completeUpTo(offset);

    } catch (Throwable t) {
        // Replay failed
        log.error("Replaying transaction marker to {} failed due to: {}.",
            tp, t.getMessage(), t);
        event.complete(t);

        transitionTo(CoordinatorState.FAILED);
        transitionTo(CoordinatorState.LOADING);

        return;
    } finally {
        asyncOperationInProgress = false;
    }

    // Flush next batch if exists
    if (currentBatch != null) {
        maybeFlushCurrentBatch(time.milliseconds());
    }
}
```

**Key Points**:
- Transaction marker follows same "write-then-replay" pattern
- No revert logic needed
- Simpler error handling

---

## State Management

### State Transitions

#### unload() - Modified

**Purpose**: Clean up when coordinator is unloaded

**Changes**: Add inFlightBatch cleanup

```java
private void unload() {
    if (highWatermarklistener != null) {
        partitionWriter.deregisterListener(tp, highWatermarklistener);
        highWatermarklistener = null;
    }
    timer.cancelAll();
    executor.cancelAll();
    deferredEventQueue.failAll(Errors.NOT_COORDINATOR.exception());

    // Handle currentBatch
    failCurrentBatch(Errors.NOT_COORDINATOR.exception());

    // NEW: Handle inFlightBatch
    if (inFlightBatch != null) {
        log.warn("Unloading coordinator {} with in-flight batch. Failing all pending events.", tp);
        inFlightBatch.deferredEvents.complete(Errors.NOT_COORDINATOR.exception());
        freeInFlightBatch();
        asyncOperationInProgress = false;
    }

    if (coordinator != null) {
        try {
            coordinator.onUnloaded();
        } catch (Throwable ex) {
            log.error("Unloading the coordinator {} failed due to: {}.", tp, ex.getMessage(), ex);
        }
        coordinator = null;
    }
}
```

**Key Point**: Must clean up inFlightBatch to prevent memory leak

#### State Transition Scenarios

1. **ACTIVE -> FAILED**:
   - Triggered by serious errors
   - `unload()` cleans up inFlightBatch
   - All pending events failed

2. **ACTIVE -> CLOSED**:
   - Triggered by shutdown
   - `unload()` cleans up inFlightBatch
   - All pending events failed

3. **FAILED -> LOADING**:
   - Triggered to reload coordinator
   - State machine reloaded from log
   - Resync with log offsets

### HWM Management

**Current Approach**: Register listener via `PartitionWriter.registerListener()`

**New Approach**: Self-manage HWM updates

**Changes**:
1. Remove `partitionWriter.registerListener(tp, highWatermarklistener)` call
2. Remove `partitionWriter.deregisterListener(tp, highWatermarklistener)` call
3. Manually call `coordinator.updateLastCommittedOffset(offset)` after state machine update
4. Manually call `deferredEventQueue.completeUpTo(offset)` after HWM update

**Rationale**:
- `appendAsync` success implies data is committed (multi-replica storage)
- No need to wait for external HWM listener
- Simpler control flow
- Avoids race condition

---

## Implementation Plan

### Phase 1: Foundation (1-2 days)

**Tasks**:
1. Add new data structures
   - `RecordToReplay` class
   - Enhance `CoordinatorBatch` with new fields
   - Add fields to `CoordinatorContext`

2. Add helper methods
   - `freeInFlightBatch()`
   - `shouldTransitionToFailed()`

3. Unit tests for new structures

**Acceptance Criteria**:
- All new code compiles
- Unit test coverage > 90%
- No impact on existing functionality

### Phase 2: Core Async Logic (3-4 days)

**Tasks**:
1. Modify `append()` method
   - Add `asyncOperationInProgress` check
   - Change to store in `recordsToReplay` instead of immediate replay
   - Keep offset allocation logic

2. Modify `flushCurrentBatch()` method
   - Change to call `appendAsync`
   - Add `whenComplete` handling

3. Implement `handleAsyncFlushCompletion()`
   - Error handling
   - Offset verification
   - Replay logic
   - HWM update
   - State cleanup

4. Modify `waitForPendingWrites()`
   - Add `inFlightBatch` check

**Acceptance Criteria**:
- Regular write requests work asynchronously
- Write failures handled correctly
- Offset verification works
- State machine updated after `appendAsync` success

### Phase 3: Transaction Support (2-3 days)

**Tasks**:
1. Modify `completeTransaction()` method
   - Change to async mode
   - Write-then-replay pattern

2. Implement `handleTransactionMarkerCompletion()`
   - Error handling
   - Replay transaction marker
   - HWM update

3. Test transaction scenarios
   - Normal commit
   - Normal abort
   - Write failures
   - Replay failures

**Acceptance Criteria**:
- Transactions commit correctly
- Transactions abort correctly
- Transaction failures handled correctly
- No revert logic needed

### Phase 4: State Management (1-2 days)

**Tasks**:
1. Modify `unload()` method
   - Add `inFlightBatch` cleanup

2. Modify `transitionTo()` method
   - Ensure correct cleanup during state transitions

3. Remove HWM listener registration
   - Remove `registerListener` call
   - Remove `deregisterListener` call

4. Test state transition scenarios
   - ACTIVE -> FAILED
   - ACTIVE -> CLOSED
   - State transitions during async operations

**Acceptance Criteria**:
- No memory leaks during state transitions
- Async operations correctly canceled or completed
- All deferred events handled correctly

### Phase 5: Integration and Stress Testing (3-5 days)

**Tasks**:
1. Integration tests
   - Multiple concurrent write requests
   - Mixed read/write requests
   - Transactional and non-transactional mixed
   - State transition scenarios

2. Stress tests
   - High throughput writes
   - Large batch writes
   - Frequent state transitions
   - Network latency simulation

3. Fault injection tests
   - Random `appendAsync` failures
   - Random replay failures
   - State transitions during async operations
   - Network partitions

4. Performance comparison
   - Throughput vs synchronous mode
   - Latency vs synchronous mode
   - Resource usage comparison

**Acceptance Criteria**:
- All integration tests pass
- Stress tests stable for > 24 hours
- No data loss in fault injection tests
- Performance improvement > 20%

### Phase 6: Documentation and Review (1-2 days)

**Tasks**:
1. Update design documentation
2. Add code comments
3. Update API documentation
4. Code review
5. Address review feedback

**Acceptance Criteria**:
- Documentation complete and clear
- Code review approved
- All feedback addressed

### Total Time Estimate: 11-18 days

---

## Testing Strategy

### Unit Tests

#### 1. Basic Async Write Tests
- `testAsyncAppendSuccess`: Normal async write
- `testAsyncAppendFailure`: `appendAsync` fails
- `testAsyncAppendOffsetMismatch`: Offset mismatch
- `testAsyncAppendReplayFailure`: Replay fails
- `testAsyncAppendStateNotActive`: State not ACTIVE

#### 2. Ordering Tests
- `testStrictSequentialFlush`: Strict sequential flush
- `testCannotAllocateBatchDuringAsync`: Cannot allocate during async
- `testWaitForPendingWrites`: Wait mechanism
- `testMultipleBatchesSequential`: Multiple batches in sequence

#### 3. Transaction Tests
- `testTransactionCommitAsync`: Async commit
- `testTransactionAbortAsync`: Async abort
- `testTransactionMarkerFailure`: Marker write fails
- `testTransactionAfterBatchFlush`: Transaction after batch flush

#### 4. State Management Tests
- `testUnloadWithInFlightBatch`: Unload with in-flight batch
- `testTransitionToFailedDuringAsync`: Transition during async
- `testTransitionToClosedDuringAsync`: Transition during async
- `testMemoryLeakPrevention`: Memory leak prevention

#### 5. HWM Update Tests
- `testHWMUpdateAfterAsyncFlush`: HWM update after async flush
- `testHWMUpdateAfterTransactionMarker`: HWM update after marker
- `testDeferredEventCompletion`: Deferred event completion

### Integration Tests

#### 1. Normal Flow Tests
- Single write request complete flow
- Multiple consecutive write requests
- Mixed read/write requests
- Transactional and non-transactional mixed

#### 2. Concurrency Tests
- Multiple concurrent write requests (should be serialized)
- Read requests during writes
- Write requests during state transitions

#### 3. Failure Recovery Tests
- Recovery after `appendAsync` failure
- Recovery after replay failure
- Recovery after state transition
- Recovery from crash (reload from log)

### Stress Tests

#### 1. High Throughput
- Sustained high write rate
- Large batches
- Many small batches

#### 2. Long Running
- Run for > 24 hours
- Monitor memory usage
- Monitor CPU usage
- Check for leaks

#### 3. Fault Injection
- Random `appendAsync` failures (10% rate)
- Random replay failures (1% rate)
- Random state transitions
- Network delays and partitions

### Performance Tests

#### 1. Throughput Comparison
- Measure writes/second vs synchronous mode
- Measure with different batch sizes
- Measure with different linger times

#### 2. Latency Comparison
- Measure p50, p99, p999 latency
- Compare with synchronous mode
- Measure under different loads

#### 3. Resource Usage
- CPU usage comparison
- Memory usage comparison
- I/O usage comparison

---

## Risks and Mitigations

### Risk 1: Performance Not as Expected

**Risk**: Async mode doesn't improve performance significantly

**Mitigation**:
- Thorough performance testing in Phase 5
- Benchmark against synchronous mode
- Profile to identify bottlenecks

**Fallback**:
- Keep synchronous mode as fallback
- Use feature flag to control async mode
- Gradual rollout

### Risk 2: Unforeseen Edge Cases

**Risk**: Discover edge cases not covered in design

**Mitigation**:
- Comprehensive fault injection testing
- Long-running stress tests
- Code review by multiple engineers

**Fallback**:
- Fix issues as discovered
- Add tests for new edge cases
- Document known limitations

### Risk 3: Conflicts with Existing Code

**Risk**: Changes conflict with concurrent development

**Mitigation**:
- Frequent integration testing
- Coordinate with other teams
- Use feature branches

**Fallback**:
- Resolve conflicts incrementally
- Rebase frequently
- Maintain backward compatibility

### Risk 4: Memory Leaks

**Risk**: `inFlightBatch` or `recordsToReplay` not freed properly

**Mitigation**:
- Careful resource management
- Memory leak detection tests
- Code review focus on cleanup paths

**Fallback**:
- Add monitoring and alerts
- Fix leaks as discovered
- Add defensive cleanup code

### Risk 5: Data Correctness Issues

**Risk**: Async mode causes data loss or corruption

**Mitigation**:
- Extensive testing with fault injection
- Verify READ COMMITTED semantics
- Test crash recovery scenarios

**Fallback**:
- Disable async mode if issues found
- Fix root cause
- Add more validation

---

## Compatibility and Migration

### Backward Compatibility

#### PartitionWriter Interface
- ✅ `appendAsync` has default implementation
- ✅ Existing implementations continue to work
- ✅ New implementations can provide true async

#### CoordinatorShard Interface
- ✅ No changes required
- ✅ `replay()` signature unchanged
- ✅ `replayEndTransactionMarker()` signature unchanged

#### External APIs
- ✅ No changes to external APIs
- ✅ Client behavior unchanged
- ✅ Better semantics (READ COMMITTED)

### Migration Strategy

#### Phase 1: Development
- Implement in feature branch
- Test thoroughly
- Code review

#### Phase 2: Testing Environment
- Deploy to test environment
- Run integration tests
- Run stress tests
- Monitor for issues

#### Phase 3: Canary Deployment
- Deploy to small percentage of production
- Monitor metrics closely
- Compare with synchronous mode
- Gradually increase percentage

#### Phase 4: Full Rollout
- Deploy to all production
- Monitor for issues
- Keep synchronous mode as fallback

### Rollback Plan

If issues are discovered:
1. Use feature flag to disable async mode
2. Fall back to synchronous mode
3. Investigate and fix issues
4. Re-test thoroughly
5. Retry deployment

---

## Performance Expectations

### Expected Improvements

#### Throughput
- **Target**: 20-30% improvement
- **Reason**: Reduced blocking time in event queue
- **Measurement**: Writes per second under sustained load

#### Latency
- **Target**: 10-20% reduction in p99 latency
- **Reason**: Less queueing delay
- **Measurement**: End-to-end write latency

#### Resource Utilization
- **Target**: Better CPU and I/O utilization
- **Reason**: Parallel processing of CPU and I/O
- **Measurement**: CPU usage, I/O wait time

### Potential Overheads

#### Memory
- **Overhead**: `recordsToReplay` list storage
- **Estimate**: ~100 bytes per record
- **Mitigation**: Bounded by batch size

#### Event Queue
- **Overhead**: Additional completion events
- **Estimate**: 1 event per batch
- **Mitigation**: Minimal compared to other events

#### Complexity
- **Overhead**: More complex code
- **Mitigation**: Thorough testing and documentation

---

## Open Questions

### Q1: Should we support configurable async mode?

**Question**: Should async mode be configurable, or always enabled?

**Options**:
- A) Always async (simpler)
- B) Configurable via flag (safer)

**Recommendation**: Start with configurable flag for safer rollout

### Q2: Should we add metrics for async operations?

**Question**: What metrics should we add?

**Suggestions**:
- Async operation duration
- In-flight batch count
- Async failure rate
- Replay duration

**Recommendation**: Add comprehensive metrics for monitoring

### Q3: Should we support batch pipelining in future?

**Question**: Should we consider pipelining multiple batches in future?

**Current**: Strict sequential (one in-flight at a time)
**Future**: Pipeline multiple batches (more complex)

**Recommendation**: Start with strict sequential, consider pipelining later

---

## Conclusion

This design provides a comprehensive plan for converting `CoordinatorRuntime` to use asynchronous `appendAsync()` calls. The key benefits are:

1. **READ COMMITTED Semantics**: Data only visible after successful persistence
2. **Non-blocking**: Event queue not blocked by I/O operations
3. **Simpler Error Handling**: No revert logic needed
4. **Better Performance**: Expected 20-30% throughput improvement
5. **Production Ready**: Comprehensive testing and rollback plan

The implementation follows a phased approach with clear acceptance criteria at each phase. The design has been carefully reviewed for correctness, concurrency, and memory safety.

---

## Appendix A: Code Modification Summary

### Files to Modify

1. **CoordinatorRuntime.java**
   - Add `RecordToReplay` class
   - Modify `CoordinatorBatch` class
   - Add fields to `CoordinatorContext`
   - Modify `maybeAllocateNewBatch()`
   - Modify `append()`
   - Modify `flushCurrentBatch()`
   - Add `handleAsyncFlushCompletion()`
   - Modify `waitForPendingWrites()`
   - Modify `completeTransaction()`
   - Add `handleTransactionMarkerCompletion()`
   - Modify `unload()`
   - Add `freeInFlightBatch()`
   - Add `shouldTransitionToFailed()`
   - Remove HWM listener registration

### Estimated Lines of Code

- New code: ~500 lines
- Modified code: ~300 lines
- Deleted code: ~50 lines
- Test code: ~1000 lines

### Impact Analysis

- **Write path**: Significant changes
- **Read path**: No changes
- **State transitions**: Minor changes
- **Recovery path**: No changes

---

## Appendix B: Sequence Diagrams

### Normal Write Flow

```
User Request
    |
    v
append()
    |-- Check asyncOperationInProgress
    |-- Allocate batch if needed
    |-- Assign offsets
    |-- Store in recordsToReplay
    |-- Add to builder
    v
maybeFlushCurrentBatch()
    |
    v
flushCurrentBatch()
    |-- Set asyncOperationInProgress = true
    |-- Move to inFlightBatch
    |-- Call appendAsync()
    |
    v
[Async Write to Storage]
    |
    v
whenComplete callback
    |
    v
enqueueLast(AsyncFlushCompletionEvent)
    |
    v
handleAsyncFlushCompletion()
    |-- Verify offset
    |-- Replay to state machine
    |-- Update lastWrittenOffset
    |-- Update lastCommittedOffset
    |-- Complete deferred events
    |-- Set asyncOperationInProgress = false
    v
Done
```

### Transaction Completion Flow

```
completeTransaction()
    |-- Check asyncOperationInProgress
    |-- flushCurrentBatch() -> wait
    |-- Check again
    |-- Set asyncOperationInProgress = true
    |-- Call appendAsync(marker)
    v
[Async Write Marker]
    |
    v
whenComplete callback
    |
    v
enqueueLast(TransactionMarkerCompletionEvent)
    |
    v
handleTransactionMarkerCompletion()
    |-- Replay transaction marker
    |-- Update lastWrittenOffset
    |-- Update lastCommittedOffset
    |-- Complete deferred events
    |-- Set asyncOperationInProgress = false
    v
Done
```

### Error Handling Flow

```
appendAsync() fails
    |
    v
handleAsyncFlushCompletion(throwable)
    |-- Fail deferred events
    |-- Free inFlightBatch
    |-- Set asyncOperationInProgress = false
    |-- If serious -> transition to FAILED
    v
Done (no revert needed)
```

---

## Appendix C: Glossary

- **READ COMMITTED**: Isolation level where reads only see committed data
- **In-flight batch**: Batch currently being written asynchronously
- **Deferred event**: Event waiting for write to complete
- **HWM**: High Water Mark, highest committed offset
- **State machine**: Coordinator's in-memory state
- **Replay**: Apply record to state machine
- **Revert**: Undo state machine changes (not needed in new design)

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-07 | Design Session | Initial design document |

---

**End of Document**
