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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.AsyncTransactionIndex;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.apache.bookkeeper.client.MockedBookKeeperTestCase;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Properties;

public class BookkeeperLocalLogBasicTest extends MockedBookKeeperTestCase {
    @Test
    public void testBasicBookkeeperLocalLog() throws Exception {
        MetadataStoreExtended metadataStore = getMetadataStore();
        ManagedLedgerFactory factory = getFactory();

        TopicPartition topicPartition = new TopicPartition("testBasicBookkeeperLocalLog", 1);
        AsyncTransactionIndex asyncTransactionIndex = new AsyncTransactionIndex(metadataStore, topicPartition);
        asyncTransactionIndex.recoverSnapshotAsync().get();
        Assertions.assertEquals(-1, asyncTransactionIndex.mapEndOffset());

        BookkeeperLocalLog bookkeeperLocalLog = new BookkeeperLocalLog(new LogConfig(new Properties()), new KafkaScheduler(1), topicPartition, asyncTransactionIndex);
        bookkeeperLocalLog.initializeAsync(factory).get();

        Assertions.assertEquals(0L, bookkeeperLocalLog.logEndOffsetMetadata().messageOffset);
        for (int i = 0; i < 10; i++) {
            LogAppendInfo appendInfo = Mockito.mock(LogAppendInfo.class);
            Mockito.when(appendInfo.numMessages()).thenReturn(1L);
            bookkeeperLocalLog.appendAsync(appendInfo, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(i, "test".getBytes()))).get();
        }
        Assertions.assertEquals(10L, bookkeeperLocalLog.logEndOffset());

        FetchDataInfo fetchDataInfo = bookkeeperLocalLog.readAsync(0, Integer.MAX_VALUE, true, null, false).get();
        Assertions.assertEquals(0L, fetchDataInfo.fetchOffsetMetadata.messageOffset);

        long offset = -1;
        int totalMessages = 0;
        for (RecordBatch recordBatch : fetchDataInfo.records.batches()) {
            long baseOffset = recordBatch.baseOffset();
            long lastOffset = recordBatch.lastOffset();
            totalMessages += (int) (lastOffset - baseOffset + 1);
            // Verify offset continuity
            Assertions.assertEquals(offset + 1, baseOffset);
            offset = lastOffset;
        }
        Assertions.assertEquals(10, totalMessages);
    }
}
