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
 * Apache BookKeeper integration for Kafka log storage.
 *
 * <p>This package provides an alternative storage backend for Kafka that uses
 * Apache BookKeeper for distributed, replicated log storage. The implementation
 * leverages Apache Pulsar's managed-ledger module for high-level abstractions.
 *
 * <h2>Key Components</h2>
 * <ul>
 *   <li>{@link org.apache.kafka.storage.internals.log.bookkeeper.BookkeeperUnifiedLog} -
 *       Extends UnifiedLog with async read/write operations for BookKeeper</li>
 *   <li>{@link org.apache.kafka.storage.internals.log.bookkeeper.BookkeeperConfig} -
 *       Configuration options for BookKeeper integration</li>
 * </ul>
 *
 * <h2>Architecture</h2>
 * <pre>
 * Kafka Broker
 *      │
 *      ▼
 * BookkeeperUnifiedLog (extends UnifiedLog)
 *      │
 *      ├── asyncAppend() ──▶ ManagedLedger.asyncAddEntry()
 *      │
 *      └── asyncRead() ───▶ ManagedCursor.asyncReadEntries()
 *              │
 *              ▼
 *          BookKeeper Cluster (Bookies)
 * </pre>
 *
 * <h2>Key Design Decisions</h2>
 * <ul>
 *   <li><b>Async I/O:</b> All BookKeeper operations are asynchronous since they
 *       involve network I/O to remote bookie nodes.</li>
 *   <li><b>ISR Handling:</b> BookKeeper handles replication internally via quorum
 *       writes, so Kafka's ISR mechanism can be optionally disabled.</li>
 *   <li><b>Offset Mapping:</b> Kafka's contiguous offsets are mapped to BookKeeper's
 *       (ledgerId, entryId) addressing scheme.</li>
 *   <li><b>Index Handling:</b> Traditional file-based indexes (offset, timestamp)
 *       are not used; instead, queries go through BookKeeper/ManagedLedger.</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <pre>
 * # Enable BookKeeper storage
 * log.storage.type=bookkeeper
 *
 * # BookKeeper connection
 * bookkeeper.metadata.service.uri=zk://localhost:2181/ledgers
 *
 * # Replication settings (BookKeeper handles this, not Kafka ISR)
 * bookkeeper.ensemble.size=3
 * bookkeeper.write.quorum.size=2
 * bookkeeper.ack.quorum.size=2
 *
 * # Disable Kafka ISR since BookKeeper handles replication
 * bookkeeper.disable.isr.tracking=true
 * </pre>
 *
 * <h2>Idempotency Considerations</h2>
 * <p>Since writes are asynchronous, the idempotent producer mechanism needs careful
 * consideration:
 * <ul>
 *   <li>Producer state must be updated atomically with writes</li>
 *   <li>Duplicate detection must work across async boundaries</li>
 *   <li>Transaction markers must be properly ordered</li>
 * </ul>
 *
 * @see <a href="https://bookkeeper.apache.org/">Apache BookKeeper</a>
 * @see <a href="https://github.com/apache/pulsar/tree/master/managed-ledger">Pulsar ManagedLedger</a>
 * @see <a href="https://github.com/streamnative/kop">KoP (Kafka on Pulsar)</a>
 */
package org.apache.kafka.storage.internals.log.bookkeeper;
