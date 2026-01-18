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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

public class BookkeeperStorageSingleton {
    private final LogConfig config;
    private final EventLoopGroup ioEventLoopGroup;
    private final MetadataStoreExtended metadataStoreExtended;
    private final ManagedLedgerClientFactory managedLedgerClientFactory;
    private final BookkeeperClientFactory clientFactory;


    public BookkeeperStorageSingleton(LogConfig logConfig) throws Exception {
        this.config = logConfig;
        try {
            this.metadataStoreExtended = createMetadataStoreExtended();
        } catch (Throwable t) {
            throw new Exception("Initialize MetadataStore failed", t);
        }
        this.clientFactory = new BookkeeperClientFactoryImpl();
        this.ioEventLoopGroup = createEventLoopGroup();
        this.managedLedgerClientFactory = new ManagedLedgerClientFactory();
        this.managedLedgerClientFactory.initialize(config, metadataStoreExtended, clientFactory, ioEventLoopGroup, OpenTelemetry.noop());
    }

    private EventLoopGroup createEventLoopGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(1);
        } else {
            return new NioEventLoopGroup(1);
        }
    }

    private MetadataStoreExtended createMetadataStoreExtended() throws MetadataStoreException {
        String metadataStoreUrl = config.metadataStoreUrl;
        MetadataStoreConfig metadataStoreConfig = MetadataStoreConfig.builder()
                .metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                .build();
        return MetadataStoreExtended.create(metadataStoreUrl, metadataStoreConfig);
    }

    public LogConfig getConfig() {
        return config;
    }

    public BookkeeperClientFactory getClientFactory() {
        return clientFactory;
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerClientFactory.getManagedLedgerFactory();
    }

    public MetadataStoreExtended getMetadataStoreExtended() {
        return metadataStoreExtended;
    }

    public void close() throws Exception {
        managedLedgerClientFactory.close();
        ioEventLoopGroup.shutdownGracefully().await();
        metadataStoreExtended.close();
    }
}
