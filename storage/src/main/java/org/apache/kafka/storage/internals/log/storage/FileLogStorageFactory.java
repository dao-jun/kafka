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

import org.apache.kafka.server.log.storage.LogStorage;
import org.apache.kafka.server.log.storage.LogStorageFactory;

import java.io.File;
import java.io.IOException;

/**
 * Factory for creating FileLogStorage instances.
 * <p>
 * This is the default storage factory that creates file-based storage
 * using FileRecords.
 */
public class FileLogStorageFactory implements LogStorageFactory {

    public static final String STORAGE_TYPE = "file";

    @Override
    public LogStorage create(File file, boolean fileAlreadyExists, int initFileSize, boolean preallocate) throws IOException {
        return FileLogStorage.open(file, fileAlreadyExists, initFileSize, preallocate);
    }

    @Override
    public LogStorage open(File file, boolean mutable, boolean fileAlreadyExists, int initFileSize, boolean preallocate) throws IOException {
        return FileLogStorage.open(file, mutable, fileAlreadyExists, initFileSize, preallocate);
    }

    @Override
    public String storageType() {
        return STORAGE_TYPE;
    }
}
