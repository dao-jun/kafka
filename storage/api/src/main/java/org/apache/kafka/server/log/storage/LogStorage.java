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
package org.apache.kafka.server.log.storage;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An abstraction for log storage that can be implemented by different storage backends.
 * <p>
 * The default implementation is file-based storage using {@code FileRecords}.
 * Alternative implementations can use distributed storage systems like BookKeeper.
 * <p>
 * This interface defines the core operations needed for storing and retrieving
 * Kafka records, including append, read, truncation, and lifecycle management.
 */
public interface LogStorage extends Closeable {

    /**
     * Append records to the storage.
     *
     * @param records The records to append
     * @return The number of bytes appended
     * @throws IOException if an I/O error occurs
     */
    long append(MemoryRecords records) throws IOException;

    /**
     * Read records from the storage starting at the given position.
     *
     * @param position The byte position to start reading from
     * @param size The number of bytes to read
     * @return A ByteBuffer containing the read data
     * @throws IOException if an I/O error occurs
     */
    ByteBuffer read(int position, int size) throws IOException;

    /**
     * Slice a portion of the log for reading.
     *
     * @param position The byte position to start reading from
     * @param size The number of bytes to read
     * @return A MemoryRecords instance containing the slice
     * @throws IOException if an I/O error occurs
     */
    MemoryRecords slice(int position, int size) throws IOException;

    /**
     * Get the size of the storage in bytes.
     *
     * @return The size in bytes
     */
    int sizeInBytes();

    /**
     * Search for the offset position in the log starting from the given position.
     *
     * @param targetOffset The offset to search for
     * @param startingPosition The byte position to start searching from
     * @return The log offset position containing the offset, position, and size
     * @throws IOException if an I/O error occurs
     */
    LogOffsetPosition searchForOffsetFromPosition(long targetOffset, int startingPosition) throws IOException;

    /**
     * Search for a timestamp in the log starting from the given position.
     *
     * @param targetTimestamp The timestamp to search for
     * @param startingPosition The byte position to start searching from
     * @param startingOffset The starting offset for the search
     * @return The timestamp and offset if found, null otherwise
     * @throws IOException if an I/O error occurs
     */
    TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition, long startingOffset) throws IOException;

    /**
     * Get the largest timestamp after the given position.
     *
     * @param position The byte position to start from
     * @return The largest timestamp and offset found
     * @throws IOException if an I/O error occurs
     */
    TimestampAndOffset largestTimestampAfter(int position) throws IOException;

    /**
     * Truncate the storage to the given size.
     *
     * @param targetSize The target size in bytes
     * @return The number of bytes truncated
     * @throws IOException if an I/O error occurs
     */
    int truncateTo(int targetSize) throws IOException;

    /**
     * Trim the storage to remove any preallocated but unused space.
     *
     * @throws IOException if an I/O error occurs
     */
    void trim() throws IOException;

    /**
     * Flush the storage to ensure durability.
     *
     * @throws IOException if an I/O error occurs
     */
    void flush() throws IOException;

    /**
     * Get an iterator over the record batches in this storage.
     *
     * @return An iterable of record batches
     */
    Iterable<? extends RecordBatch> batches();

    /**
     * Get an iterator over the record batches starting from the given position.
     *
     * @param start The byte position to start from
     * @return An iterable of record batches
     * @throws IOException if an I/O error occurs
     */
    Iterable<? extends RecordBatch> batchesFrom(int start) throws IOException;

    /**
     * Rename the underlying storage file (if applicable).
     *
     * @param file The new file to rename to
     * @throws IOException if an I/O error occurs
     */
    void renameTo(File file) throws IOException;

    /**
     * Get the underlying file (if applicable).
     *
     * @return The file, or null if not file-based
     */
    File file();

    /**
     * Update the parent directory (if applicable).
     *
     * @param parentDir The new parent directory
     */
    void updateParentDir(File parentDir);

    /**
     * Delete this storage if it exists.
     *
     * @return true if deleted, false if it didn't exist
     * @throws IOException if an I/O error occurs
     */
    boolean deleteIfExists() throws IOException;

    /**
     * Close the storage handlers without writing to disk.
     * Used when the storage directory is offline.
     *
     * @throws IOException if an I/O error occurs
     */
    void closeHandlers() throws IOException;

    /**
     * Read data into the provided buffer at the given position.
     *
     * @param buffer The buffer to read into
     * @param position The position to read from
     * @throws IOException if an I/O error occurs
     */
    void readInto(ByteBuffer buffer, int position) throws IOException;

    /**
     * Represents a position in the log with offset and size information.
     */
    record LogOffsetPosition(long offset, int position, int size) {
    }

    /**
     * Represents a timestamp and its associated offset.
     */
    record TimestampAndOffset(long timestamp, long offset) {
    }
}
