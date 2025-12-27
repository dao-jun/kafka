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
package org.apache.kafka.storage.internals.log.bookkeeper;

import java.util.Properties;

/**
 * Configuration for BookKeeper-backed log storage.
 *
 * <p>This class holds all configuration options needed to connect to and
 * interact with a BookKeeper cluster through Pulsar's ManagedLedger.
 *
 * <p><b>Key Configuration Categories:</b>
 * <ul>
 *   <li>Connection settings: Metadata service URI, ZooKeeper connection</li>
 *   <li>Replication settings: Ensemble size, write quorum, ack quorum</li>
 *   <li>ManagedLedger settings: Max entries/size per ledger, rollover time</li>
 *   <li>ISR settings: Whether to disable Kafka's ISR tracking</li>
 * </ul>
 */
public class BookkeeperConfig {

    // Connection settings
    public static final String METADATA_SERVICE_URI_CONFIG = "bookkeeper.metadata.service.uri";
    public static final String METADATA_SERVICE_URI_DOC = "The metadata service URI for BookKeeper (e.g., zk://localhost:2181/ledgers)";
    public static final String METADATA_SERVICE_URI_DEFAULT = "zk://localhost:2181/ledgers";

    // Ensemble and quorum settings - these control BookKeeper's built-in replication
    public static final String ENSEMBLE_SIZE_CONFIG = "bookkeeper.ensemble.size";
    public static final String ENSEMBLE_SIZE_DOC = "The number of bookies to use for each ledger (ensemble size)";
    public static final int ENSEMBLE_SIZE_DEFAULT = 3;

    public static final String WRITE_QUORUM_SIZE_CONFIG = "bookkeeper.write.quorum.size";
    public static final String WRITE_QUORUM_SIZE_DOC = "The number of bookies to write each entry to (write quorum)";
    public static final int WRITE_QUORUM_SIZE_DEFAULT = 2;

    public static final String ACK_QUORUM_SIZE_CONFIG = "bookkeeper.ack.quorum.size";
    public static final String ACK_QUORUM_SIZE_DOC = "The number of bookies that must acknowledge a write (ack quorum)";
    public static final int ACK_QUORUM_SIZE_DEFAULT = 2;

    // ManagedLedger settings
    public static final String MAX_ENTRIES_PER_LEDGER_CONFIG = "bookkeeper.managed.ledger.max.entries";
    public static final String MAX_ENTRIES_PER_LEDGER_DOC = "Maximum number of entries per ledger before rolling to a new one";
    public static final long MAX_ENTRIES_PER_LEDGER_DEFAULT = 50000;

    public static final String MAX_SIZE_PER_LEDGER_CONFIG = "bookkeeper.managed.ledger.max.size.bytes";
    public static final String MAX_SIZE_PER_LEDGER_DOC = "Maximum size in bytes per ledger before rolling to a new one";
    public static final long MAX_SIZE_PER_LEDGER_DEFAULT = 100 * 1024 * 1024; // 100MB

    // ISR settings - since BookKeeper handles replication, Kafka's ISR may be disabled
    public static final String DISABLE_ISR_TRACKING_CONFIG = "bookkeeper.disable.isr.tracking";
    public static final String DISABLE_ISR_TRACKING_DOC = "Disable Kafka's ISR tracking since BookKeeper handles replication";
    public static final boolean DISABLE_ISR_TRACKING_DEFAULT = true;

    // Read settings
    public static final String READ_CACHE_SIZE_CONFIG = "bookkeeper.read.cache.size.bytes";
    public static final String READ_CACHE_SIZE_DOC = "Size of the read cache in bytes for BookKeeper entries";
    public static final long READ_CACHE_SIZE_DEFAULT = 64 * 1024 * 1024; // 64MB

    private final String metadataServiceUri;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private final long maxEntriesPerLedger;
    private final long maxSizePerLedger;
    private final boolean disableIsrTracking;
    private final long readCacheSize;

    /**
     * Create a BookkeeperConfig with default values.
     */
    public BookkeeperConfig() {
        this(new Properties());
    }

    /**
     * Create a BookkeeperConfig from properties.
     *
     * @param props The configuration properties
     */
    public BookkeeperConfig(Properties props) {
        this.metadataServiceUri = props.getProperty(METADATA_SERVICE_URI_CONFIG, METADATA_SERVICE_URI_DEFAULT);
        this.ensembleSize = Integer.parseInt(props.getProperty(ENSEMBLE_SIZE_CONFIG, String.valueOf(ENSEMBLE_SIZE_DEFAULT)));
        this.writeQuorumSize = Integer.parseInt(props.getProperty(WRITE_QUORUM_SIZE_CONFIG, String.valueOf(WRITE_QUORUM_SIZE_DEFAULT)));
        this.ackQuorumSize = Integer.parseInt(props.getProperty(ACK_QUORUM_SIZE_CONFIG, String.valueOf(ACK_QUORUM_SIZE_DEFAULT)));
        this.maxEntriesPerLedger = Long.parseLong(props.getProperty(MAX_ENTRIES_PER_LEDGER_CONFIG, String.valueOf(MAX_ENTRIES_PER_LEDGER_DEFAULT)));
        this.maxSizePerLedger = Long.parseLong(props.getProperty(MAX_SIZE_PER_LEDGER_CONFIG, String.valueOf(MAX_SIZE_PER_LEDGER_DEFAULT)));
        this.disableIsrTracking = Boolean.parseBoolean(props.getProperty(DISABLE_ISR_TRACKING_CONFIG, String.valueOf(DISABLE_ISR_TRACKING_DEFAULT)));
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
        if (ensembleSize < 1) {
            throw new IllegalArgumentException("Ensemble size must be at least 1");
        }
        if (ackQuorumSize < 1) {
            throw new IllegalArgumentException("Ack quorum size must be at least 1");
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

    public boolean disableIsrTracking() {
        return disableIsrTracking;
    }

    public long readCacheSize() {
        return readCacheSize;
    }

    @Override
    public String toString() {
        return "BookkeeperConfig{" +
                "metadataServiceUri='" + metadataServiceUri + '\'' +
                ", ensembleSize=" + ensembleSize +
                ", writeQuorumSize=" + writeQuorumSize +
                ", ackQuorumSize=" + ackQuorumSize +
                ", maxEntriesPerLedger=" + maxEntriesPerLedger +
                ", maxSizePerLedger=" + maxSizePerLedger +
                ", disableIsrTracking=" + disableIsrTracking +
                ", readCacheSize=" + readCacheSize +
                '}';
    }
}
