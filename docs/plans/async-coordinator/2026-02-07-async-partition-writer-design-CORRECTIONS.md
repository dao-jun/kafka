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

# Async PartitionWriter Design - Critical Corrections

## Document Information

- **Date**: 2026-02-07
- **Status**: Corrections to Original Design
- **Based on**: Code Review by general-purpose agent (a98944a)

## Critical Issues Found and Corrections

### Issue 1: HWM Management - FUNDAMENTAL DESIGN FLAW ❌

**Original Design (WRONG)**:
```java
// In handleAsyncFlushCompletion()
coordinator.updateLastCommittedOffset(offset);  // ❌ WRONG!
deferredEventQueue.completeUpTo(offset);
```

**Problem**:
- Assumes `appendAsync` success means HWM is updated
- In reality, HWM requires follower synchronization
- Violates READ COMMITTED semantics
- May cause data loss on leader failover

**CORRECTED Design**:
```java
// In handleAsyncFlushCompletion()
// Only update lastWrittenOffset, NOT lastCommittedOffset
coordinator.updateLastWrittenOffset(offset);

// Do NOT update HWM here
// Do NOT complete deferred events here

// Add batch's deferred events to queue (will be completed when HWM advances)
deferredEventQueue.add(offset, batch.deferredEvents);

// KEEP HighWatermarkListener mechanism
// HWM will be updated by PartitionWriter.Listener.onHighWatermarkUpdated()
```

**Key Changes**:
1. ✅ Keep `PartitionWriter.registerListener()` call
2. ✅ Keep `HighWatermarkListener` class
3. ✅ Only update `lastWrittenOffset` in async completion
4. ✅ Let `onHighWatermarkUpdated()` handle HWM and deferred event completion

**Updated Flow**:
```
1. appendAsync() succeeds
2. handleAsyncFlushCompletion():
   - Replay records to state machine
   - Update lastWrittenOffset
   - Add deferred events to queue (NOT complete yet)
3. PartitionWriter triggers onHighWatermarkUpdated()
4. onHighWatermarkUpdated():
   - Update lastCommittedOffset
   - Complete deferred events up to HWM
```

---

### Issue 2: Offset Allocation Logic Error ❌

**Original Design (WRONG)**:
```java
// In maybeAllocateNewBatch()
if (asyncOperationInProgress || inFlightBatch != null) {
    throw new IllegalStateException(...);
}
long baseOffset = coordinator.lastWrittenOffset() + 1;  // ❌ WRONG!
```

**Problem**:
- If `inFlightBatch` exists, `lastWrittenOffset()` hasn't been updated yet
- New batch's baseOffset may overlap with inFlightBatch's offset range

**CORRECTED Design**:
```java
// In maybeAllocateNewBatch()
if (asyncOperationInProgress || inFlightBatch != null) {
    throw new IllegalStateException(
        "Cannot allocate new batch while async operation in progress for " + tp
    );
}

// Calculate baseOffset correctly
long baseOffset = inFlightBatch != null ?
    inFlightBatch.nextOffset :  // Use inFlightBatch's next offset
    coordinator.lastWrittenOffset() + 1;  // Or last written offset

currentBatch = new CoordinatorBatch(..., baseOffset, ...);
```

**Wait, there's a logical issue**: If we throw exception when `inFlightBatch != null`, we never reach the baseOffset calculation. So the original check is actually preventing the problem!

**Actually CORRECT approach**: The exception prevents allocation, so we never have the overlap problem. But we need to ensure the check is correct:

```java
// In maybeAllocateNewBatch()
if (currentBatch == null) {
    // CRITICAL: Cannot allocate if async operation in progress
    // because lastWrittenOffset hasn't been updated yet
    if (asyncOperationInProgress || inFlightBatch != null) {
        throw new IllegalStateException(
            "Cannot allocate new batch while async operation in progress for " + tp
        );
    }

    // Now safe to use lastWrittenOffset (previous batch is complete)
    long baseOffset = coordinator.lastWrittenOffset() + 1;
    currentBatch = new CoordinatorBatch(..., baseOffset, ...);
}
```

**Conclusion**: Original design is actually CORRECT for this part. The exception prevents the problem.

---

### Issue 3: Deferred Events Completion Order ❌

**Original Design (WRONG)**:
```java
// In handleAsyncFlushCompletion()
deferredEventQueue.completeUpTo(offset);
deferredEventQueue.add(offset, batch.deferredEvents);  // ❌ WRONG ORDER!
```

**Problem**:
- `completeUpTo` is called before `add`
- Events are added after completion check
- But more importantly, should NOT complete here at all (see Issue 1)

**CORRECTED Design**:
```java
// In handleAsyncFlushCompletion()
// Do NOT call completeUpTo here
// Just add events to queue
deferredEventQueue.add(offset, batch.deferredEvents);

// Events will be completed when onHighWatermarkUpdated() is called
```

---

### Issue 4: Concurrent Access to enqueueLast ⚠️

**Original Design (INCOMPLETE)**:
```java
appendFuture.whenComplete((offset, throwable) -> {
    enqueueLast(new CoordinatorInternalEvent(...));  // May throw exception
});
```

**Problem**:
- `enqueueLast()` may throw `RejectedExecutionException`
- Exception is swallowed in `whenComplete`
- Resources not cleaned up

**CORRECTED Design**:
```java
appendFuture.whenComplete((offset, throwable) -> {
    try {
        enqueueLast(new CoordinatorInternalEvent(
            "AsyncFlushCompletion",
            tp,
            () -> handleAsyncFlushCompletion(batchToFlush, offset, throwable)
        ));
    } catch (RejectedExecutionException e) {
        // Event queue closed, log and cleanup
        log.warn("Failed to enqueue async flush completion for {} due to: {}. " +
            "Coordinator may be shutting down.", tp, e.getMessage());
        
        // Try to cleanup resources (best effort)
        // Note: This runs in I/O thread, not event queue thread
        // So we can't safely access coordinator state
        // Just log the issue - unload() will clean up
    } catch (Throwable t) {
        log.error("Unexpected error in async flush completion callback for {}", tp, t);
    }
});
```

---

### Issue 5: Offset Verification Logic ⚠️

**Original Design (POTENTIALLY WRONG)**:
```java
if (offset != batch.nextOffset) {  // May be off by one?
    // Error handling
}
```

**Analysis**:
- `batch.nextOffset` is the NEXT offset to allocate
- `offset` returned by `appendAsync` is the LAST offset written
- So `offset` should equal `batch.nextOffset - 1`?

**Need to verify**: Check what `partitionWriter.append()` currently returns:

Looking at current code (line 705-710):
```java
long offset = partitionWriter.append(...);
// ...
if (offset != currentBatch.nextOffset) {
    // Error
}
```

So current implementation expects `offset == nextOffset`. This means `append()` returns the offset AFTER the last record (i.e., the next offset to use).

**CORRECTED Design**: Original is actually CORRECT. Keep as is:
```java
if (offset != batch.nextOffset) {
    // Error: state machine out of sync
}
```

---

## Updated Core Methods

### handleAsyncFlushCompletion() - CORRECTED

```java
private void handleAsyncFlushCompletion(
    CoordinatorBatch batch,
    Long offset,
    Throwable throwable
) {
    if (throwable != null) {
        log.error("Async write to {} failed due to: {}.", tp, throwable.getMessage(), throwable);
        batch.deferredEvents.complete(throwable);
        freeInFlightBatch();
        asyncOperationInProgress = false;
        
        if (shouldTransitionToFailed(throwable)) {
            transitionTo(CoordinatorState.FAILED);
            transitionTo(CoordinatorState.LOADING);
        }
        return;
    }

    log.debug("Async write to {} completed successfully with offset {}.", tp, offset);

    // Verify offset
    if (offset != batch.nextOffset) {
        log.error("State machine out of sync: expected offset {}, got {}",
            batch.nextOffset, offset);
        batch.deferredEvents.complete(Errors.NOT_COORDINATOR.exception());
        freeInFlightBatch();
        asyncOperationInProgress = false;
        transitionTo(CoordinatorState.FAILED);
        transitionTo(CoordinatorState.LOADING);
        return;
    }

    // Check state
    if (state != CoordinatorState.ACTIVE) {
        log.warn("Coordinator {} no longer active (state={})", tp, state);
        batch.deferredEvents.complete(Errors.NOT_COORDINATOR.exception());
        freeInFlightBatch();
        asyncOperationInProgress = false;
        return;
    }

    try {
        // Replay records to state machine
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

        // Update lastWrittenOffset (NOT lastCommittedOffset)
        coordinator.updateLastWrittenOffset(offset);

        // Add deferred events to queue (will be completed when HWM advances)
        // DO NOT call completeUpTo here
        deferredEventQueue.add(offset, batch.deferredEvents);

        // Note: HWM will be updated by HighWatermarkListener.onHighWatermarkUpdated()
        // which will then call deferredEventQueue.completeUpTo()

    } catch (Throwable t) {
        log.error("Replaying records to {} failed due to: {}.", tp, t.getMessage(), t);
        batch.deferredEvents.complete(t);
        freeInFlightBatch();
        asyncOperationInProgress = false;
        transitionTo(CoordinatorState.FAILED);
        transitionTo(CoordinatorState.LOADING);
        return;
    }

    // Clean up
    freeInFlightBatch();
    asyncOperationInProgress = false;

    // Flush next batch if exists
    if (currentBatch != null) {
        maybeFlushCurrentBatch(time.milliseconds());
    }
}
```

### handleTransactionMarkerCompletion() - CORRECTED

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
        log.error("Async write of transaction marker to {} failed", tp, throwable);
        asyncOperationInProgress = false;
        event.complete(throwable);
        
        if (shouldTransitionToFailed(throwable)) {
            transitionTo(CoordinatorState.FAILED);
            transitionTo(CoordinatorState.LOADING);
        }
        return;
    }

    log.debug("Transaction marker written to {} at offset {}.", tp, offset);

    if (state != CoordinatorState.ACTIVE) {
        log.warn("Coordinator {} no longer active", tp);
        asyncOperationInProgress = false;
        event.complete(Errors.NOT_COORDINATOR.exception());
        return;
    }

    try {
        // Replay transaction marker to state machine
        coordinator.replayEndTransactionMarker(producerId, producerEpoch, result);

        // Update lastWrittenOffset (NOT lastCommittedOffset)
        coordinator.updateLastWrittenOffset(offset);

        // Add deferred event to queue (will be completed when HWM advances)
        deferredEventQueue.add(offset, DeferredEventCollection.of(log, event));

        // Note: HWM will be updated by HighWatermarkListener.onHighWatermarkUpdated()

    } catch (Throwable t) {
        log.error("Replaying transaction marker failed", tp, t);
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

---

## Updated Design Principles

### 3. High Watermark Management (CORRECTED)

**Principle**: HWM is managed by PartitionWriter, NOT by CoordinatorRuntime.

**Implementation**:
- ✅ KEEP `HighWatermarkListener` registration via `PartitionWriter.registerListener()`
- ✅ KEEP `onHighWatermarkUpdated()` callback
- ✅ In `handleAsyncFlushCompletion()`: only update `lastWrittenOffset`
- ✅ In `onHighWatermarkUpdated()`: update `lastCommittedOffset` and complete deferred events

**Rationale**:
- `appendAsync` success means data is written to leader
- HWM update requires follower synchronization
- PartitionWriter knows when HWM advances
- Separating write completion from HWM update is correct

**Flow**:
```
1. appendAsync() succeeds -> data written to leader
2. handleAsyncFlushCompletion():
   - Replay to state machine (data visible for reads)
   - Update lastWrittenOffset
   - Add events to deferred queue
3. Followers sync data
4. PartitionWriter detects HWM advance
5. onHighWatermarkUpdated():
   - Update lastCommittedOffset
   - Complete deferred events (responses sent to clients)
```

---

## Impact on READ COMMITTED Semantics

**Question**: Does this still achieve READ COMMITTED?

**Answer**: YES, but with clarification:

1. **State Machine Visibility**: 
   - Data is visible in state machine after `handleAsyncFlushCompletion()`
   - This happens AFTER `appendAsync` succeeds (data written to leader)
   - ✅ Better than current (which makes data visible before write)

2. **Client Response**:
   - Client responses are sent after HWM advances
   - HWM advance means data is replicated
   - ✅ This is true READ COMMITTED

3. **Internal Reads**:
   - Internal reads (within coordinator) see data after replay
   - External reads (via API) wait for HWM
   - ✅ Correct separation

**Conclusion**: The corrected design achieves READ COMMITTED semantics correctly.

---

## Summary of Corrections

| Issue | Original Design | Corrected Design | Severity |
|-------|----------------|------------------|----------|
| HWM Management | Self-managed, update in completion | Keep HighWatermarkListener | 🔴 Critical |
| Deferred Events | Complete in completion handler | Complete in HWM callback | 🔴 Critical |
| Offset Allocation | Actually correct (exception prevents issue) | No change needed | ✅ OK |
| Error Handling | Missing try-catch in callback | Add try-catch | 🟡 Important |
| Offset Verification | Actually correct | No change needed | ✅ OK |

---

## Action Items

1. ✅ Update design document with corrected HWM management
2. ✅ Update `handleAsyncFlushCompletion()` implementation
3. ✅ Update `handleTransactionMarkerCompletion()` implementation
4. ✅ Keep `HighWatermarkListener` registration
5. ✅ Add error handling in `whenComplete` callbacks
6. ⚠️ Verify transaction semantics with coordinator implementations
7. ⚠️ Ensure all `replay()` methods are idempotent
8. ⚠️ Add comprehensive tests for HWM and deferred event completion

---

**End of Corrections Document**
