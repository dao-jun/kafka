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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link BookKeeperConfig}.
 */
class BookKeeperConfigTest {

    @Test
    void testDefaultConfiguration() {
        BookKeeperConfig config = new BookKeeperConfig();
        
        assertEquals(BookKeeperConfig.METADATA_SERVICE_URI_DEFAULT, config.metadataServiceUri());
        assertEquals(BookKeeperConfig.ENSEMBLE_SIZE_DEFAULT, config.ensembleSize());
        assertEquals(BookKeeperConfig.WRITE_QUORUM_SIZE_DEFAULT, config.writeQuorumSize());
        assertEquals(BookKeeperConfig.ACK_QUORUM_SIZE_DEFAULT, config.ackQuorumSize());
        assertEquals(BookKeeperConfig.MAX_ENTRIES_PER_LEDGER_DEFAULT, config.maxEntriesPerLedger());
        assertEquals(BookKeeperConfig.MAX_SIZE_PER_LEDGER_DEFAULT, config.maxSizePerLedger());
        assertEquals(BookKeeperConfig.MIN_ROLLOVER_TIME_DEFAULT, config.minRolloverTimeMs());
        assertEquals(BookKeeperConfig.READ_CACHE_SIZE_DEFAULT, config.readCacheSize());
    }

    @Test
    void testCustomConfiguration() {
        Properties props = new Properties();
        props.setProperty(BookKeeperConfig.METADATA_SERVICE_URI_CONFIG, "zk://custom:2181/ledgers");
        props.setProperty(BookKeeperConfig.ENSEMBLE_SIZE_CONFIG, "5");
        props.setProperty(BookKeeperConfig.WRITE_QUORUM_SIZE_CONFIG, "3");
        props.setProperty(BookKeeperConfig.ACK_QUORUM_SIZE_CONFIG, "2");
        props.setProperty(BookKeeperConfig.MAX_ENTRIES_PER_LEDGER_CONFIG, "100000");
        
        BookKeeperConfig config = new BookKeeperConfig(props);
        
        assertEquals("zk://custom:2181/ledgers", config.metadataServiceUri());
        assertEquals(5, config.ensembleSize());
        assertEquals(3, config.writeQuorumSize());
        assertEquals(2, config.ackQuorumSize());
        assertEquals(100000, config.maxEntriesPerLedger());
    }

    @Test
    void testWriteQuorumValidation() {
        Properties props = new Properties();
        props.setProperty(BookKeeperConfig.ENSEMBLE_SIZE_CONFIG, "3");
        props.setProperty(BookKeeperConfig.WRITE_QUORUM_SIZE_CONFIG, "5"); // Invalid: larger than ensemble
        
        assertThrows(IllegalArgumentException.class, () -> new BookKeeperConfig(props));
    }

    @Test
    void testAckQuorumValidation() {
        Properties props = new Properties();
        props.setProperty(BookKeeperConfig.ENSEMBLE_SIZE_CONFIG, "5");
        props.setProperty(BookKeeperConfig.WRITE_QUORUM_SIZE_CONFIG, "3");
        props.setProperty(BookKeeperConfig.ACK_QUORUM_SIZE_CONFIG, "4"); // Invalid: larger than write quorum
        
        assertThrows(IllegalArgumentException.class, () -> new BookKeeperConfig(props));
    }

    @Test
    void testToString() {
        BookKeeperConfig config = new BookKeeperConfig();
        String str = config.toString();
        
        // Verify toString contains key configuration values
        org.junit.jupiter.api.Assertions.assertTrue(str.contains("metadataServiceUri"));
        org.junit.jupiter.api.Assertions.assertTrue(str.contains("ensembleSize"));
        org.junit.jupiter.api.Assertions.assertTrue(str.contains("writeQuorumSize"));
    }
}
