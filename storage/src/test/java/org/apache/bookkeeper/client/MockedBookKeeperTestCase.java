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
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class MockedBookKeeperTestCase {

    static final Logger LOG = LoggerFactory.getLogger(MockedBookKeeperTestCase.class);

    // BookKeeper related variables
    protected PulsarMockBookKeeper bkc;
    protected int numBookies;

    protected ManagedLedgerFactoryImpl factory;

    protected static OrderedScheduler executor;
    protected static ExecutorService cachedExecutor;

    protected FaultInjectionMetadataStore metadataStore;

    public MockedBookKeeperTestCase() {
        // By default start a 3 bookies cluster
        this(3);
    }

    public MockedBookKeeperTestCase(int numBookies) {
        this.numBookies = numBookies;
    }

    @BeforeEach
    public final void setUp(TestInfo testInfo) throws Exception {
        LOG.info(">>>>>> starting {}", testInfo.getDisplayName());
        metadataStore = new FaultInjectionMetadataStore(
                MetadataStoreExtended.create("memory:local",
                        MetadataStoreConfig.builder().metadataStoreName("metastore-" + testInfo.getTestMethod().map(Method::getName).orElse("unknown")).build()));

        try {
            // start bookkeeper service
            startBookKeeper();
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }

        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        initManagedLedgerFactoryConfig(managedLedgerFactoryConfig);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        initManagedLedgerConfig(managedLedgerConfig);
        factory =
                new ManagedLedgerFactoryImpl(metadataStore, bkc, managedLedgerFactoryConfig, managedLedgerConfig);

        setUpTestCase();
    }

    public ManagedLedgerFactory getFactory() {
        return factory;
    }

    public MetadataStoreExtended getMetadataStore() {
        return metadataStore;
    }

    protected ManagedLedgerConfig initManagedLedgerConfig(ManagedLedgerConfig config) {
        config.setCacheEvictionByExpectedReadCount(false);
        return config;
    }

    protected void initManagedLedgerFactoryConfig(ManagedLedgerFactoryConfig config) {
        // increase default cache eviction interval so that caching could be tested with less flakyness
        config.setCacheEvictionIntervalMs(200);
    }

    protected void setUpTestCase() throws Exception {

    }

    @AfterEach
    public final void tearDown(TestInfo testInfo) {
        try {
            cleanUpTestCase();
        } catch (Exception e) {
            LOG.error("tearDown Error", e);
        }
        try {
            LOG.info("@@@@@@@@@ stopping " + testInfo.getDisplayName());
            if (factory != null) {
                try {
                    factory.shutdownAsync().get(10, TimeUnit.SECONDS);
                } catch (ManagedLedgerException.ManagedLedgerFactoryClosedException e) {
                    // ignore
                }
                factory = null;
            }
            stopBookKeeper();
            if (metadataStore != null) {
                metadataStore.close();
                metadataStore = null;
            }
            LOG.info("--------- stopped {}", testInfo.getDisplayName());
        } catch (Exception e) {
            LOG.error("tearDown Error", e);
        }
    }

    protected void cleanUpTestCase() throws Exception {

    }

    @BeforeAll
    public static void setUpClass() {
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(2).name("test").build();
        cachedExecutor = Executors.newCachedThreadPool();
    }

    @AfterAll
    public static void tearDownClass() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (cachedExecutor != null) {
            cachedExecutor.shutdownNow();
        }
    }

    /**
     * Start cluster.
     *
     * @throws Exception
     */
    protected void startBookKeeper() throws Exception {
        for (int i = 0; i < numBookies; i++) {
            metadataStore.put("/ledgers/available/192.168.1.1:" + (5000 + i), new byte[0], Optional.empty()).join();
        }

        metadataStore.put("/ledgers/LAYOUT", "1\nflat:1".getBytes(), Optional.empty()).join();

        bkc = new PulsarMockBookKeeper(executor);
    }

    protected void stopBookKeeper() {
        if (bkc != null) {
            bkc.shutdown();
            bkc = null;
        }
    }

    protected void stopMetadataStore() {
        metadataStore.setAlwaysFail(new MetadataStoreException("failed"));
    }
}