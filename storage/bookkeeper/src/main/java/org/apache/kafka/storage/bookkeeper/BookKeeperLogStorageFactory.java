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

import org.apache.kafka.server.log.storage.LogStorage;
import org.apache.kafka.server.log.storage.LogStorageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for creating BookKeeperLogStorage instances.
 * <p>
 * This factory manages the lifecycle of BookKeeper storage instances
 * and provides integration with the Kafka storage layer.
 * <p>
 * When fully implemented, this factory will:
 * <ul>
 *   <li>Initialize connection to BookKeeper cluster</li>
 *   <li>Create and manage ManagedLedgerFactory</li>
 *   <li>Handle ledger creation and recovery</li>
 *   <li>Manage offset-to-entry mappings</li>
 * </ul>
 */
public class BookKeeperLogStorageFactory implements LogStorageFactory {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperLogStorageFactory.class);
    public static final String STORAGE_TYPE = "bookkeeper";

    private final BookKeeperConfig config;
    private final ConcurrentMap<String, BookKeeperLogStorage> storages;
    private volatile boolean initialized = false;
    private volatile boolean shuttingDown = false;

    // TODO: Add these fields when integrating with actual BookKeeper/Pulsar ML
    // private ManagedLedgerFactory mlFactory;
    // private BookKeeper bookKeeper;

    /**
     * Create a BookKeeperLogStorageFactory with the given configuration.
     *
     * @param config The BookKeeper configuration
     */
    public BookKeeperLogStorageFactory(BookKeeperConfig config) {
        this.config = config;
        this.storages = new ConcurrentHashMap<>();
    }

    /**
     * Initialize the factory and establish connection to BookKeeper.
     *
     * @throws IOException if initialization fails
     */
    public synchronized void initialize() throws IOException {
        if (!initialized) {
            LOG.info("Initializing BookKeeperLogStorageFactory with config: {}", config);
            // TODO: Initialize BookKeeper client and ManagedLedgerFactory
            // ClientConfiguration clientConfig = new ClientConfiguration()
            //     .setMetadataServiceUri(config.metadataServiceUri());
            // bookKeeper = BookKeeper.forConfig(clientConfig).build();
            // mlFactory = new ManagedLedgerFactoryImpl(bookKeeper);
            initialized = true;
            LOG.info("BookKeeperLogStorageFactory initialized successfully");
        }
    }

    @Override
    public LogStorage create(File file, boolean fileAlreadyExists, int initFileSize, boolean preallocate) throws IOException {
        ensureInitialized();
        String ledgerName = deriveLedgerName(file);
        return createOrGetStorage(ledgerName);
    }

    @Override
    public LogStorage open(File file, boolean mutable, boolean fileAlreadyExists, int initFileSize, boolean preallocate) throws IOException {
        ensureInitialized();
        String ledgerName = deriveLedgerName(file);
        return createOrGetStorage(ledgerName);
    }

    @Override
    public String storageType() {
        return STORAGE_TYPE;
    }

    /**
     * Get the BookKeeper configuration.
     *
     * @return The configuration
     */
    public BookKeeperConfig config() {
        return config;
    }

    /**
     * Derive a ledger name from a file path.
     * The file path typically contains topic-partition information.
     *
     * @param file The file that would be used in file-based storage
     * @return The ledger name for BookKeeper
     */
    private String deriveLedgerName(File file) {
        // Extract topic-partition information from the file path
        // Format: /data/kafka-logs/topic-partition/segment.log
        String name = file.getName();
        String parent = file.getParent();
        if (parent != null) {
            File parentFile = new File(parent);
            return parentFile.getName() + "/" + name;
        }
        return name;
    }

    private BookKeeperLogStorage createOrGetStorage(String ledgerName) throws IOException {
        if (shuttingDown) {
            throw new IOException("Factory is shutting down, cannot create new storage");
        }
        return storages.computeIfAbsent(ledgerName, name -> {
            LOG.info("Creating BookKeeperLogStorage for: {}", name);
            return new BookKeeperLogStorage(name, config);
        });
    }

    private void ensureInitialized() throws IOException {
        if (!initialized) {
            initialize();
        }
    }

    /**
     * Close the factory and all managed storage instances.
     *
     * @throws IOException if an error occurs during shutdown
     */
    public synchronized void close() throws IOException {
        if (shuttingDown) {
            return;
        }
        shuttingDown = true;
        
        LOG.info("Closing BookKeeperLogStorageFactory");
        for (BookKeeperLogStorage storage : storages.values()) {
            try {
                storage.close();
            } catch (IOException e) {
                LOG.warn("Error closing storage: {}", storage.ledgerName(), e);
            }
        }
        storages.clear();
        
        // TODO: Close ManagedLedgerFactory and BookKeeper client
        // if (mlFactory != null) mlFactory.shutdown();
        // if (bookKeeper != null) bookKeeper.close();
        
        initialized = false;
        LOG.info("BookKeeperLogStorageFactory closed");
    }
}
