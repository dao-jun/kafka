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

import java.io.File;
import java.io.IOException;

/**
 * Factory interface for creating LogStorage instances.
 * <p>
 * Implementations of this interface can create different types of storage backends,
 * such as file-based storage (FileRecords) or distributed storage (BookKeeper).
 */
public interface LogStorageFactory {

    /**
     * Create a new LogStorage instance.
     *
     * @param file The file or directory for storage
     * @param fileAlreadyExists Whether the file already exists
     * @param initFileSize The initial file size for preallocation
     * @param preallocate Whether to preallocate the file
     * @return A new LogStorage instance
     * @throws IOException if an I/O error occurs
     */
    LogStorage create(File file, boolean fileAlreadyExists, int initFileSize, boolean preallocate) throws IOException;

    /**
     * Open an existing LogStorage instance.
     *
     * @param file The file to open
     * @param mutable Whether the storage should be mutable
     * @param fileAlreadyExists Whether the file already exists
     * @param initFileSize The initial file size for preallocation
     * @param preallocate Whether to preallocate the file
     * @return A LogStorage instance
     * @throws IOException if an I/O error occurs
     */
    LogStorage open(File file, boolean mutable, boolean fileAlreadyExists, int initFileSize, boolean preallocate) throws IOException;

    /**
     * Get the type of storage this factory creates.
     *
     * @return The storage type identifier
     */
    String storageType();
}
