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
package org.apache.kafka.storage.bookkeeper;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Maps Kafka offsets to BookKeeper ledger/entry positions.
 * <p>
 * Kafka uses contiguous 64-bit offsets, while BookKeeper uses (ledgerId, entryId) tuples.
 * This class provides the mapping between these two addressing schemes.
 * <p>
 * Mapping strategies:
 * <ul>
 *   <li><b>Simple composition:</b> Combine ledgerId and entryId into a single long value</li>
 *   <li><b>Offset tracking:</b> Maintain a mapping from Kafka offset ranges to (ledgerId, entryId)</li>
 * </ul>
 * <p>
 * This implementation uses offset tracking for flexibility and compatibility with
 * existing Kafka consumers that expect contiguous offsets.
 *
 * @see <a href="https://github.com/streamnative/kop">KoP for reference implementation</a>
 */
public class OffsetMapper {

    // Constants for composite offset calculation
    private static final int LEDGER_ID_SHIFT = 32;
    private static final long ENTRY_ID_MASK = 0xFFFFFFFFL;

    /**
     * Represents a position in BookKeeper storage.
     *
     * @param ledgerId The ledger ID
     * @param entryId The entry ID within the ledger
     */
    public record BookKeeperPosition(long ledgerId, long entryId) {
        
        /**
         * Check if this is a valid position.
         *
         * @return true if both ledgerId and entryId are non-negative
         */
        public boolean isValid() {
            return ledgerId >= 0 && entryId >= 0;
        }

        @Override
        public String toString() {
            return "(" + ledgerId + ":" + entryId + ")";
        }
    }

    /**
     * Represents a range of Kafka offsets mapped to a BookKeeper position.
     */
    private record OffsetRange(long startOffset, long endOffset, BookKeeperPosition position) {
    }

    // Maps Kafka offset to BookKeeper position
    // Key: start offset of the entry, Value: (ledgerId, entryId)
    private final NavigableMap<Long, BookKeeperPosition> offsetToPosition;
    
    // Current ledger being written to
    private volatile long currentLedgerId = -1;
    private volatile long currentEntryId = -1;
    private volatile long nextOffset = 0;

    /**
     * Create a new OffsetMapper.
     */
    public OffsetMapper() {
        this.offsetToPosition = new ConcurrentSkipListMap<>();
    }

    /**
     * Create a new OffsetMapper with a starting offset.
     *
     * @param startOffset The initial offset
     */
    public OffsetMapper(long startOffset) {
        this();
        this.nextOffset = startOffset;
    }

    /**
     * Record a mapping from a Kafka offset to a BookKeeper position.
     *
     * @param kafkaOffset The Kafka offset
     * @param position The BookKeeper position
     */
    public void recordMapping(long kafkaOffset, BookKeeperPosition position) {
        offsetToPosition.put(kafkaOffset, position);
        if (position.ledgerId > currentLedgerId || 
            (position.ledgerId == currentLedgerId && position.entryId > currentEntryId)) {
            currentLedgerId = position.ledgerId;
            currentEntryId = position.entryId;
        }
        if (kafkaOffset >= nextOffset) {
            nextOffset = kafkaOffset + 1;
        }
    }

    /**
     * Get the BookKeeper position for a Kafka offset.
     *
     * @param kafkaOffset The Kafka offset to look up
     * @return The corresponding BookKeeper position, or empty if not found
     */
    public Optional<BookKeeperPosition> getPosition(long kafkaOffset) {
        // Find the entry that contains this offset
        var entry = offsetToPosition.floorEntry(kafkaOffset);
        if (entry != null) {
            return Optional.of(entry.getValue());
        }
        return Optional.empty();
    }

    /**
     * Get the next Kafka offset to be assigned.
     *
     * @return The next offset
     */
    public long getNextOffset() {
        return nextOffset;
    }

    /**
     * Allocate the next offset for a new record.
     *
     * @param position The BookKeeper position for the new record
     * @return The allocated Kafka offset
     */
    public long allocateOffset(BookKeeperPosition position) {
        long offset = nextOffset++;
        recordMapping(offset, position);
        return offset;
    }

    /**
     * Get the current ledger ID being written to.
     *
     * @return The current ledger ID, or -1 if not set
     */
    public long getCurrentLedgerId() {
        return currentLedgerId;
    }

    /**
     * Get the current entry ID within the current ledger.
     *
     * @return The current entry ID, or -1 if not set
     */
    public long getCurrentEntryId() {
        return currentEntryId;
    }

    /**
     * Set the current ledger being written to.
     * Called when rolling to a new ledger.
     *
     * @param ledgerId The new ledger ID
     */
    public void setCurrentLedger(long ledgerId) {
        this.currentLedgerId = ledgerId;
        this.currentEntryId = -1;
    }

    /**
     * Truncate the offset mappings to the given offset.
     * All mappings at or after the given offset will be removed.
     *
     * @param offset The offset to truncate to
     */
    public void truncateTo(long offset) {
        offsetToPosition.tailMap(offset, true).clear();
        nextOffset = offset;
    }

    /**
     * Get the number of offset mappings.
     *
     * @return The number of mappings
     */
    public int size() {
        return offsetToPosition.size();
    }

    /**
     * Clear all mappings.
     */
    public void clear() {
        offsetToPosition.clear();
        currentLedgerId = -1;
        currentEntryId = -1;
        nextOffset = 0;
    }

    /**
     * Convert a composite offset back to a BookKeeper position.
     * This is an alternative simple mapping strategy where:
     * offset = (ledgerId << LEDGER_ID_SHIFT) | entryId
     *
     * @param compositeOffset The composite offset
     * @return The BookKeeper position
     */
    public static BookKeeperPosition fromCompositeOffset(long compositeOffset) {
        long ledgerId = compositeOffset >>> LEDGER_ID_SHIFT;
        long entryId = compositeOffset & ENTRY_ID_MASK;
        return new BookKeeperPosition(ledgerId, entryId);
    }

    /**
     * Create a composite offset from a BookKeeper position.
     * This is an alternative simple mapping strategy where:
     * offset = (ledgerId << LEDGER_ID_SHIFT) | entryId
     *
     * @param position The BookKeeper position
     * @return The composite offset
     */
    public static long toCompositeOffset(BookKeeperPosition position) {
        return (position.ledgerId << LEDGER_ID_SHIFT) | (position.entryId & ENTRY_ID_MASK);
    }
}
