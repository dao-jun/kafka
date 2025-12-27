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

import java.util.Properties;

/**
 * Configuration for BookKeeper-based log storage.
 * <p>
 * This class holds all configuration options needed to connect to and
 * interact with a BookKeeper cluster, including connection settings,
 * replication parameters, and managed ledger options.
 */
public class BookKeeperConfig {

    // BookKeeper connection settings
    public static final String METADATA_SERVICE_URI_CONFIG = "bookkeeper.metadata.service.uri";
    public static final String METADATA_SERVICE_URI_DOC = "The metadata service URI for BookKeeper (e.g., zk://localhost:2181/ledgers)";
    public static final String METADATA_SERVICE_URI_DEFAULT = "zk://localhost:2181/ledgers";

    // Ensemble and quorum settings
    public static final String ENSEMBLE_SIZE_CONFIG = "bookkeeper.ensemble.size";
    public static final String ENSEMBLE_SIZE_DOC = "The number of bookies to use for each ledger";
    public static final int ENSEMBLE_SIZE_DEFAULT = 3;

    public static final String WRITE_QUORUM_SIZE_CONFIG = "bookkeeper.write.quorum.size";
    public static final String WRITE_QUORUM_SIZE_DOC = "The number of bookies to write to for each entry";
    public static final int WRITE_QUORUM_SIZE_DEFAULT = 2;

    public static final String ACK_QUORUM_SIZE_CONFIG = "bookkeeper.ack.quorum.size";
    public static final String ACK_QUORUM_SIZE_DOC = "The number of bookies that must acknowledge a write";
    public static final int ACK_QUORUM_SIZE_DEFAULT = 2;

    // Managed ledger settings
    public static final String MAX_ENTRIES_PER_LEDGER_CONFIG = "bookkeeper.managed.ledger.max.entries";
    public static final String MAX_ENTRIES_PER_LEDGER_DOC = "Maximum number of entries per ledger before rolling";
    public static final long MAX_ENTRIES_PER_LEDGER_DEFAULT = 50000;

    public static final String MAX_SIZE_PER_LEDGER_CONFIG = "bookkeeper.managed.ledger.max.size.bytes";
    public static final String MAX_SIZE_PER_LEDGER_DOC = "Maximum size in bytes per ledger before rolling";
    public static final long MAX_SIZE_PER_LEDGER_DEFAULT = 100 * 1024 * 1024; // 100MB

    public static final String MIN_ROLLOVER_TIME_CONFIG = "bookkeeper.managed.ledger.min.rollover.time.ms";
    public static final String MIN_ROLLOVER_TIME_DOC = "Minimum time in milliseconds before rolling to a new ledger";
    public static final long MIN_ROLLOVER_TIME_DEFAULT = 600000; // 10 minutes

    // Read settings
    public static final String READ_CACHE_SIZE_CONFIG = "bookkeeper.read.cache.size.bytes";
    public static final String READ_CACHE_SIZE_DOC = "Size of the read cache in bytes";
    public static final long READ_CACHE_SIZE_DEFAULT = 64 * 1024 * 1024; // 64MB

    private final String metadataServiceUri;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private final long maxEntriesPerLedger;
    private final long maxSizePerLedger;
    private final long minRolloverTimeMs;
    private final long readCacheSize;

    /**
     * Create a BookKeeperConfig with default values.
     */
    public BookKeeperConfig() {
        this(new Properties());
    }

    /**
     * Create a BookKeeperConfig from properties.
     *
     * @param props The configuration properties
     */
    public BookKeeperConfig(Properties props) {
        this.metadataServiceUri = props.getProperty(METADATA_SERVICE_URI_CONFIG, METADATA_SERVICE_URI_DEFAULT);
        this.ensembleSize = Integer.parseInt(props.getProperty(ENSEMBLE_SIZE_CONFIG, String.valueOf(ENSEMBLE_SIZE_DEFAULT)));
        this.writeQuorumSize = Integer.parseInt(props.getProperty(WRITE_QUORUM_SIZE_CONFIG, String.valueOf(WRITE_QUORUM_SIZE_DEFAULT)));
        this.ackQuorumSize = Integer.parseInt(props.getProperty(ACK_QUORUM_SIZE_CONFIG, String.valueOf(ACK_QUORUM_SIZE_DEFAULT)));
        this.maxEntriesPerLedger = Long.parseLong(props.getProperty(MAX_ENTRIES_PER_LEDGER_CONFIG, String.valueOf(MAX_ENTRIES_PER_LEDGER_DEFAULT)));
        this.maxSizePerLedger = Long.parseLong(props.getProperty(MAX_SIZE_PER_LEDGER_CONFIG, String.valueOf(MAX_SIZE_PER_LEDGER_DEFAULT)));
        this.minRolloverTimeMs = Long.parseLong(props.getProperty(MIN_ROLLOVER_TIME_CONFIG, String.valueOf(MIN_ROLLOVER_TIME_DEFAULT)));
        this.readCacheSize = Long.parseLong(props.getProperty(READ_CACHE_SIZE_CONFIG, String.valueOf(READ_CACHE_SIZE_DEFAULT)));

        validate();
    }

    private void validate() {
        if (writeQuorumSize > ensembleSize) {
            throw new IllegalArgumentException("Write quorum size (" + writeQuorumSize + 
                    ") cannot be greater than ensemble size (" + ensembleSize + ")");
        }
        if (ackQuorumSize > writeQuorumSize) {
            throw new IllegalArgumentException("Ack quorum size (" + ackQuorumSize + 
                    ") cannot be greater than write quorum size (" + writeQuorumSize + ")");
        }
    }

    public String metadataServiceUri() {
        return metadataServiceUri;
    }

    public int ensembleSize() {
        return ensembleSize;
    }

    public int writeQuorumSize() {
        return writeQuorumSize;
    }

    public int ackQuorumSize() {
        return ackQuorumSize;
    }

    public long maxEntriesPerLedger() {
        return maxEntriesPerLedger;
    }

    public long maxSizePerLedger() {
        return maxSizePerLedger;
    }

    public long minRolloverTimeMs() {
        return minRolloverTimeMs;
    }

    public long readCacheSize() {
        return readCacheSize;
    }

    @Override
    public String toString() {
        return "BookKeeperConfig{" +
                "metadataServiceUri='" + metadataServiceUri + '\'' +
                ", ensembleSize=" + ensembleSize +
                ", writeQuorumSize=" + writeQuorumSize +
                ", ackQuorumSize=" + ackQuorumSize +
                ", maxEntriesPerLedger=" + maxEntriesPerLedger +
                ", maxSizePerLedger=" + maxSizePerLedger +
                ", minRolloverTimeMs=" + minRolloverTimeMs +
                ", readCacheSize=" + readCacheSize +
                '}';
    }
}
