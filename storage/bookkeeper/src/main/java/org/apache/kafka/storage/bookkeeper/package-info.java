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

/**
 * BookKeeper-based storage implementation for Apache Kafka.
 * <p>
 * This package provides an alternative storage backend for Kafka that uses
 * Apache BookKeeper for distributed, replicated log storage. The implementation
 * leverages Apache Pulsar's managed-ledger module for high-level abstractions.
 *
 * <h2>Key Components</h2>
 * <ul>
 *   <li>{@link org.apache.kafka.storage.bookkeeper.BookKeeperLogStorage} - 
 *       The main storage implementation</li>
 *   <li>{@link org.apache.kafka.storage.bookkeeper.BookKeeperLogStorageFactory} - 
 *       Factory for creating storage instances</li>
 *   <li>{@link org.apache.kafka.storage.bookkeeper.BookKeeperConfig} - 
 *       Configuration options for BookKeeper integration</li>
 *   <li>{@link org.apache.kafka.storage.bookkeeper.OffsetMapper} - 
 *       Handles Kafka offset to BookKeeper entry mapping</li>
 * </ul>
 *
 * <h2>Architecture</h2>
 * <pre>
 * Kafka Log API
 *      │
 *      ▼
 * LogStorage Interface (storage-api)
 *      │
 *      ├── FileLogStorage (default, file-based)
 *      │
 *      └── BookKeeperLogStorage (distributed)
 *              │
 *              ▼
 *          ManagedLedger (Pulsar)
 *              │
 *              ▼
 *          BookKeeper Cluster
 * </pre>
 *
 * <h2>Usage</h2>
 * To use BookKeeper storage, configure the following in server.properties:
 * <pre>
 * storage.type=bookkeeper
 * bookkeeper.metadata.service.uri=zk://localhost:2181/ledgers
 * bookkeeper.ensemble.size=3
 * bookkeeper.write.quorum.size=2
 * bookkeeper.ack.quorum.size=2
 * </pre>
 *
 * <h2>Dependencies</h2>
 * When fully implemented, this module requires:
 * <ul>
 *   <li>Apache Pulsar managed-ledger</li>
 *   <li>Apache BookKeeper client</li>
 * </ul>
 *
 * @see <a href="https://bookkeeper.apache.org/">Apache BookKeeper</a>
 * @see <a href="https://pulsar.apache.org/">Apache Pulsar</a>
 * @see <a href="https://github.com/streamnative/kop">KoP (Kafka on Pulsar)</a>
 */
package org.apache.kafka.storage.bookkeeper;
