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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

public class SubscriptionReceiveProcessorSupplier<KLeft, KRight>
    implements ProcessorSupplier<KRight, SubscriptionWrapper<KLeft>, CombinedKey<KRight, KLeft>, Change<ValueAndTimestamp<SubscriptionWrapper<KLeft>>>> {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionReceiveProcessorSupplier.class);

    private final StoreFactory subscriptionStoreFactory;
    private final CombinedKeySchema<KRight, KLeft> keySchema;

    public SubscriptionReceiveProcessorSupplier(final StoreFactory subscriptionStoreFactory,
                                                final CombinedKeySchema<KRight, KLeft> keySchema) {
        this.subscriptionStoreFactory = subscriptionStoreFactory;
        this.keySchema = keySchema;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(new FactoryWrappingStoreBuilder<>(subscriptionStoreFactory));
    }

    @Override
    public Processor<KRight, SubscriptionWrapper<KLeft>, CombinedKey<KRight, KLeft>, Change<ValueAndTimestamp<SubscriptionWrapper<KLeft>>>> get() {
        return new ContextualProcessor<>() {
            private TimestampedKeyValueStore<Bytes, SubscriptionWrapper<KLeft>> store;
            private Sensor droppedRecordsSensor;

            @Override
            public void init(final ProcessorContext<CombinedKey<KRight, KLeft>, Change<ValueAndTimestamp<SubscriptionWrapper<KLeft>>>> context) {
                super.init(context);
                final InternalProcessorContext<?, ?> internalProcessorContext = (InternalProcessorContext<?, ?>) context;

                droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                    Thread.currentThread().getName(),
                    internalProcessorContext.taskId().toString(),
                    internalProcessorContext.metrics()
                );
                store = internalProcessorContext.getStateStore(subscriptionStoreFactory.storeName());

                keySchema.init(context);
            }

            @Override
            public void process(final Record<KRight, SubscriptionWrapper<KLeft>> record) {
                if (record.key() == null && !SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE.equals(record.value().instruction())) {
                    dropRecord();
                    return;
                }
                if (record.value().version() > SubscriptionWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionWrapper. Need to ensure that there is compatibility
                    //with previous versions to enable rolling upgrades. Must develop a strategy for upgrading
                    //from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionWrapper is of an incompatible version.");
                }
                context().forward(
                    record.withKey(new CombinedKey<>(record.key(), record.value().primaryKey()))
                        .withValue(inferChange(record))
                        .withTimestamp(record.timestamp())
                );
            }

            private Change<ValueAndTimestamp<SubscriptionWrapper<KLeft>>> inferChange(final Record<KRight, SubscriptionWrapper<KLeft>> record) {
                if (record.key() == null) {
                    return new Change<>(ValueAndTimestamp.make(record.value(), record.timestamp()), null);
                } else {
                    return inferBasedOnState(record);
                }
            }

            private Change<ValueAndTimestamp<SubscriptionWrapper<KLeft>>> inferBasedOnState(final Record<KRight, SubscriptionWrapper<KLeft>> record) {
                final Bytes subscriptionKey = keySchema.toBytes(record.key(), record.value().primaryKey());

                final ValueAndTimestamp<SubscriptionWrapper<KLeft>> newValue = ValueAndTimestamp.make(record.value(), record.timestamp());
                final ValueAndTimestamp<SubscriptionWrapper<KLeft>> oldValue = store.get(subscriptionKey);

                //This store is used by the prefix scanner in ForeignTableJoinProcessorSupplier
                if (record.value().instruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE) ||
                    record.value().instruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE)) {
                    store.delete(subscriptionKey);
                } else {
                    store.put(subscriptionKey, newValue);
                }
                return new Change<>(newValue, oldValue);
            }

            private void dropRecord() {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null foreign key. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null foreign key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
            }
        };
    }
}