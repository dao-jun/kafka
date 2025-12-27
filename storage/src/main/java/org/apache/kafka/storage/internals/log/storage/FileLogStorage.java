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
package org.apache.kafka.storage.internals.log.storage;

import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.server.log.storage.LogStorage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A LogStorage implementation that uses FileRecords for file-based storage.
 * <p>
 * This is the default storage implementation that wraps the existing FileRecords
 * class to provide backward compatibility while enabling alternative storage backends.
 */
public class FileLogStorage implements LogStorage {

    private final FileRecords fileRecords;

    /**
     * Create a FileLogStorage wrapping existing FileRecords.
     *
     * @param fileRecords The FileRecords instance to wrap
     */
    public FileLogStorage(FileRecords fileRecords) {
        this.fileRecords = fileRecords;
    }

    /**
     * Open a FileLogStorage for the given file.
     *
     * @param file The file to open
     * @param mutable Whether the file is mutable
     * @param fileAlreadyExists Whether the file already exists
     * @param initFileSize Initial file size
     * @param preallocate Whether to preallocate the file
     * @return A new FileLogStorage instance
     * @throws IOException if an I/O error occurs
     */
    public static FileLogStorage open(File file, boolean mutable, boolean fileAlreadyExists,
                                      int initFileSize, boolean preallocate) throws IOException {
        FileRecords records = FileRecords.open(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        return new FileLogStorage(records);
    }

    /**
     * Open a FileLogStorage for the given file with default settings.
     *
     * @param file The file to open
     * @param fileAlreadyExists Whether the file already exists
     * @param initFileSize Initial file size
     * @param preallocate Whether to preallocate the file
     * @return A new FileLogStorage instance
     * @throws IOException if an I/O error occurs
     */
    public static FileLogStorage open(File file, boolean fileAlreadyExists,
                                      int initFileSize, boolean preallocate) throws IOException {
        FileRecords records = FileRecords.open(file, fileAlreadyExists, initFileSize, preallocate);
        return new FileLogStorage(records);
    }

    /**
     * Get the underlying FileRecords instance.
     * This method is provided for backward compatibility during migration.
     *
     * @return The wrapped FileRecords instance
     */
    public FileRecords fileRecords() {
        return fileRecords;
    }

    @Override
    public long append(MemoryRecords records) throws IOException {
        return fileRecords.append(records);
    }

    @Override
    public ByteBuffer read(int position, int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        fileRecords.readInto(buffer, position);
        buffer.flip();
        return buffer;
    }

    @Override
    public MemoryRecords slice(int position, int size) throws IOException {
        FileRecords sliced = fileRecords.slice(position, size);
        // Convert FileRecords slice to MemoryRecords for consistent return type
        ByteBuffer buffer = ByteBuffer.allocate(size);
        sliced.readInto(buffer, 0);
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    @Override
    public int sizeInBytes() {
        return fileRecords.sizeInBytes();
    }

    @Override
    public LogOffsetPosition searchForOffsetFromPosition(long targetOffset, int startingPosition) throws IOException {
        FileRecords.LogOffsetPosition pos = fileRecords.searchForOffsetFromPosition(targetOffset, startingPosition);
        if (pos == null) {
            return null;
        }
        return new LogOffsetPosition(pos.offset, pos.position, pos.size);
    }

    @Override
    public TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition, long startingOffset) throws IOException {
        FileRecords.TimestampAndOffset result = fileRecords.searchForTimestamp(targetTimestamp, startingPosition, startingOffset);
        if (result == null) {
            return null;
        }
        return new TimestampAndOffset(result.timestamp, result.offset);
    }

    @Override
    public TimestampAndOffset largestTimestampAfter(int position) throws IOException {
        FileRecords.TimestampAndOffset result = fileRecords.largestTimestampAfter(position);
        if (result == null) {
            return null;
        }
        return new TimestampAndOffset(result.timestamp, result.offset);
    }

    @Override
    public int truncateTo(int targetSize) throws IOException {
        return fileRecords.truncateTo(targetSize);
    }

    @Override
    public void trim() throws IOException {
        fileRecords.trim();
    }

    @Override
    public void flush() throws IOException {
        fileRecords.flush();
    }

    @Override
    public Iterable<? extends RecordBatch> batches() {
        return fileRecords.batches();
    }

    @Override
    public Iterable<? extends RecordBatch> batchesFrom(int start) throws IOException {
        return fileRecords.batchesFrom(start);
    }

    @Override
    public void renameTo(File file) throws IOException {
        fileRecords.renameTo(file);
    }

    @Override
    public File file() {
        return fileRecords.file();
    }

    @Override
    public void updateParentDir(File parentDir) {
        fileRecords.updateParentDir(parentDir);
    }

    @Override
    public boolean deleteIfExists() throws IOException {
        return fileRecords.deleteIfExists();
    }

    @Override
    public void closeHandlers() throws IOException {
        fileRecords.closeHandlers();
    }

    @Override
    public void readInto(ByteBuffer buffer, int position) throws IOException {
        fileRecords.readInto(buffer, position);
    }

    @Override
    public void close() throws IOException {
        fileRecords.close();
    }
}
