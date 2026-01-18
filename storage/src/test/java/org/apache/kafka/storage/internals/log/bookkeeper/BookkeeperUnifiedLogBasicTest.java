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

import org.apache.bookkeeper.client.MockedBookKeeperTestCase;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.AsyncProducerStateManager;
import org.apache.kafka.storage.internals.log.AsyncTransactionIndex;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BookkeeperUnifiedLogBasicTest extends MockedBookKeeperTestCase {

    @Test
    public void bookkeeperUnifiedLogBasicTest() throws Exception {
        MetadataStoreExtended metadataStore = getMetadataStore();
        ManagedLedgerFactory factory = getFactory();

        TopicPartition tp = new TopicPartition("bookkeeperUnifiedLogBasicTest", 0);
        LogConfig logConfig = new LogConfig(new Properties());
        KafkaScheduler scheduler = new KafkaScheduler(1);
        AsyncTransactionIndex transactionIndex = new AsyncTransactionIndex(metadataStore, tp);
        AsyncProducerStateManager stateManager = new AsyncProducerStateManager(tp, 30_000,
                new ProducerStateManagerConfig(1000, true), Time.SYSTEM, metadataStore);

        BookkeeperLocalLog bookkeeperLocalLog = new BookkeeperLocalLog(logConfig, scheduler, tp, transactionIndex);

        BookkeeperUnifiedLog bookkeeperUnifiedLog =
                bookkeeperLocalLog.initializeAsync(factory)
                        .thenApply(logStartOffset -> {
                            try {
                                return new BookkeeperUnifiedLog(logStartOffset,
                                        bookkeeperLocalLog, new BrokerTopicStats(), 1000, null,
                                        stateManager, Optional.empty(), false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .thenCompose(bk -> bk.initialize().thenApply(ignore -> bk))
                        .get();

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            SimpleRecord record = new SimpleRecord(Time.SYSTEM.milliseconds(), ("key_" + i).getBytes(), ("value_" + i).getBytes());
            bookkeeperUnifiedLog.appendAsLeaderAsync(MemoryRecords.withRecords(Compression.NONE, record),
                    0, AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL,
                    TransactionVersion.LATEST_PRODUCTION.transactionLogValueVersion())
                            .whenComplete((v, t) -> {
                                latch.countDown();
                            });
        }

        latch.await();

        long logEndOffset = bookkeeperUnifiedLog.logEndOffset();
        Assertions.assertEquals(10, logEndOffset);
        Assertions.assertEquals(0, bookkeeperUnifiedLog.logStartOffset());

        Assertions.assertEquals(10, stateManager.mapEndOffset());
        Assertions.assertEquals(10, transactionIndex.mapEndOffset());

        FetchDataInfo fetchDataInfo = bookkeeperUnifiedLog.readAsync(0, Integer.MAX_VALUE, FetchIsolation.LOG_END,  true)
                .get();

        int numOfMessages = 0;
        Assertions.assertEquals(0, fetchDataInfo.fetchOffsetMetadata.messageOffset);
        for (RecordBatch batch : fetchDataInfo.records.batches()) {
            for (Record record : batch) {
                Assertions.assertEquals(numOfMessages, record.offset());
                numOfMessages++;
            }
        }
        Assertions.assertEquals(10, numOfMessages);
    }

}
