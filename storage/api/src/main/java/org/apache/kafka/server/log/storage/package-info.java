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
 * Pluggable storage API for Apache Kafka.
 * <p>
 * This package defines the interfaces for pluggable storage backends in Kafka.
 * The primary interface is {@link org.apache.kafka.server.log.storage.LogStorage},
 * which abstracts the operations needed for log segment storage.
 *
 * <h2>Key Interfaces</h2>
 * <ul>
 *   <li>{@link org.apache.kafka.server.log.storage.LogStorage} - 
 *       Core interface for log storage operations</li>
 *   <li>{@link org.apache.kafka.server.log.storage.LogStorageFactory} - 
 *       Factory interface for creating storage instances</li>
 * </ul>
 *
 * <h2>Implementations</h2>
 * <ul>
 *   <li><b>FileLogStorage</b> - Default file-based storage using FileRecords</li>
 *   <li><b>BookKeeperLogStorage</b> - Distributed storage using Apache BookKeeper</li>
 * </ul>
 *
 * <h2>Design Goals</h2>
 * <ul>
 *   <li>Backward compatibility with existing file-based storage</li>
 *   <li>Support for distributed storage backends</li>
 *   <li>Minimal changes to existing Kafka code</li>
 *   <li>Configurable storage type per broker or topic</li>
 * </ul>
 */
package org.apache.kafka.server.log.storage;
