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

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ManagedLedgerInterceptorImpl implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImpl.class);
    private static final String INDEX = "index";
    private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

    private final AppendIndexMetadataInterceptor appendIndexMetadataInterceptor;

    public ManagedLedgerInterceptorImpl(Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors) {
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;

        // save appendIndexMetadataInterceptor to field
        AppendIndexMetadataInterceptor appendIndexMetadataInterceptor = null;

        for (BrokerEntryMetadataInterceptor interceptor : this.brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendIndexMetadataInterceptor) {
                appendIndexMetadataInterceptor = (AppendIndexMetadataInterceptor) interceptor;
                break;
            }
        }

        this.appendIndexMetadataInterceptor = appendIndexMetadataInterceptor;
    }

    public long getIndex() {
        long index = -1;

        if (appendIndexMetadataInterceptor != null) {
            return appendIndexMetadataInterceptor.getIndex();
        }

        return index;
    }

    @Override
    public void beforeAddEntry(AddEntryOperation op, int numberOfMessages) {
        if (op == null || numberOfMessages <= 0) {
            return;
        }
        op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors, numberOfMessages));
    }

    @Override
    public void afterFailedAddEntry(int numberOfMessages) {
        if (appendIndexMetadataInterceptor != null) {
            appendIndexMetadataInterceptor.decreaseWithNumberOfMessages(numberOfMessages);
        }
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.isEmpty()) {
            return;
        }

        if (propertiesMap.containsKey(INDEX)) {
            if (appendIndexMetadataInterceptor != null) {
                appendIndexMetadataInterceptor.recoveryIndexGenerator(
                        Long.parseLong(propertiesMap.get(INDEX)));
            }
        }
    }

    @Override
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LastEntryHandle lh) {
        return lh.readLastEntryAsync().thenAccept(lastEntryOptional -> {
            if (lastEntryOptional.isPresent()) {
                Entry lastEntry = lastEntryOptional.get();
                try {
                    Commands.peekBrokerEntryMetadataAndConsume(lastEntry.getDataBuffer(), brokerEntryMetadata -> {
                        if (brokerEntryMetadata != null && brokerEntryMetadata.hasIndex()) {
                            appendIndexMetadataInterceptor.recoveryIndexGenerator(brokerEntryMetadata.getIndex());
                        }
                    });
                } finally {
                    lastEntry.release();
                }
            }
        });
    }

    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        if (appendIndexMetadataInterceptor != null) {
            propertiesMap.put(INDEX, String.valueOf(appendIndexMetadataInterceptor.getIndex()));
        }
    }
}
