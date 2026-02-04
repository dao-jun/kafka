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

import org.apache.kafka.storage.internals.log.LogConfig;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;

public class ManagedLedgerClientFactory {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerClientFactory.class);

    @VisibleForTesting
    protected ManagedLedgerFactory managedLedgerFactory;
    private BookKeeper defaultBkClient;
    private final AsyncCache<EnsemblePlacementPolicyConfig, BookKeeper>
            bkEnsemblePolicyToBkClientMap = Caffeine.newBuilder().recordStats().buildAsync();

    public ManagedLedgerClientFactory() {
        CacheMetricsCollector.CAFFEINE.addCache("managed-ledger-bk-ensemble-client-cache",
                bkEnsemblePolicyToBkClientMap);
    }

    public void initialize(LogConfig conf, MetadataStoreExtended metadataStore,
                           BookkeeperClientFactory bookkeeperProvider,
                           EventLoopGroup eventLoopGroup,
                           OpenTelemetry openTelemetry) throws Exception {
        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(conf.maxCacheSizeMb * 1024L * 1024L);
        managedLedgerFactoryConfig.setCacheEvictionWatermark(conf.cacheEvictionWatermark);
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(conf.numSchedulerThreads);
        managedLedgerFactoryConfig.setCacheEvictionIntervalMs(conf.cacheEvictionIntervalMs);
        managedLedgerFactoryConfig.setCacheEvictionTimeThresholdMillis(conf.cacheEvictionTimeThresholdMs);
        // default to 2 * managedLedgerCacheEvictionTimeThresholdMillis if the value is unset
        managedLedgerFactoryConfig.setContinueCachingAddedEntriesAfterLastActiveCursorLeavesMillis(
                2L * conf.cacheEvictionTimeThresholdMs);

        managedLedgerFactoryConfig.setCopyEntriesInCache(conf.copyEntriesInCache);
        long managedLedgerMaxReadsInFlightSizeBytes = conf.maxReadInflightSizeMb * 1024L * 1024L;
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightSize(managedLedgerMaxReadsInFlightSizeBytes);
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightSize(conf.maxReadInflightSize);
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis(
                conf.maxReadInflightPermitsAcquireTimeoutMs);
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightPermitsAcquireQueueSize(
                conf.maxReadInflightPermitsAcquireQueueSize);
        managedLedgerFactoryConfig.setPrometheusStatsLatencyRolloverSeconds(
                conf.prometheusStatsLatencyRolloverSeconds);
        managedLedgerFactoryConfig.setTraceTaskExecution(conf.traceTaskExecutor);
        managedLedgerFactoryConfig.setManagedLedgerInfoCompressionType(conf.managedLedgerInfoCompressionType);
        managedLedgerFactoryConfig.setManagedLedgerInfoCompressionThresholdInBytes(
                conf.managedLedgerInfoCompressionThresholdBytes);
        managedLedgerFactoryConfig.setStatsPeriodSeconds(conf.infoStatsPeriodSeconds);

        this.defaultBkClient = bookkeeperProvider.create(conf, metadataStore, eventLoopGroup, null).get();

        ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory = (
                EnsemblePlacementPolicyConfig ensemblePlacementPolicyConfig) -> {
            if (ensemblePlacementPolicyConfig == null || ensemblePlacementPolicyConfig.getPolicyClass() == null) {
                return CompletableFuture.completedFuture(defaultBkClient);
            }

            // find or create bk-client in cache for a specific ensemblePlacementPolicy
            return bkEnsemblePolicyToBkClientMap.get(ensemblePlacementPolicyConfig,
                    (config, executor) -> bookkeeperProvider.create(
                            conf, metadataStore, eventLoopGroup,
                            ensemblePlacementPolicyConfig.getProperties(), new NullStatsLogger()));
        };

        try {
            this.managedLedgerFactory =
                    createManagedLedgerFactory(metadataStore, openTelemetry, bkFactory, managedLedgerFactoryConfig);
        } catch (Exception e) {
            defaultBkClient.close();
            throw e;
        }
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    protected ManagedLedgerFactoryImpl createManagedLedgerFactory(MetadataStoreExtended metadataStore,
                                                                  OpenTelemetry openTelemetry,
                                                                  ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy
                                                                          bkFactory,
                                                                  ManagedLedgerFactoryConfig managedLedgerFactoryConfig) throws Exception {
        return new ManagedLedgerFactoryImpl(metadataStore, bkFactory, managedLedgerFactoryConfig, new NullStatsLogger(),
                openTelemetry);
    }

    public void close() throws IOException {
        try {
            if (null != managedLedgerFactory) {
                managedLedgerFactory.shutdown();
                log.info("Closed managed ledger factory");
            }

            try {
                if (null != defaultBkClient) {
                    defaultBkClient.close();
                }
            } catch (RejectedExecutionException ree) {
                // when closing bookkeeper client, it will error outs all pending metadata operations.
                // those callbacks of those operations will be triggered, and submitted to the scheduler
                // in managed ledger factory. but the managed ledger factory has been shutdown before,
                // so `RejectedExecutionException` will be thrown there. we can safely ignore this exception.
                //
                // an alternative solution is to close bookkeeper client before shutting down managed ledger
                // factory, however that might be introducing more unknowns.
                log.warn("Encountered exceptions on closing bookkeeper client", ree);
            }
            bkEnsemblePolicyToBkClientMap.synchronous().asMap().forEach((policy, bk) -> {
                try {
                    bk.close();
                } catch (Exception e) {
                    log.warn("Failed to close bookkeeper-client for policy {}", policy, e);
                }
            });
            log.info("Closed BookKeeper client");
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }

}
