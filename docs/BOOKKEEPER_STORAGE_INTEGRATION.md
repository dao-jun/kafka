# Replacing Kafka Storage with BookKeeper (Using Pulsar's Managed-Ledger)

## 概述 (Overview)

This document provides a comprehensive guide for replacing Kafka's underlying file-based storage layer with Apache BookKeeper, leveraging Apache Pulsar's managed-ledger module. This is a significant architectural change that requires modifications to multiple core components.

## 1. Kafka Storage Architecture Analysis

### 1.1 Core Storage Components

Kafka's storage layer consists of the following key components:

| Component | Location | Description |
|-----------|----------|-------------|
| **UnifiedLog** | `storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java` | Presents a unified view of local and tiered log segments. Handles state and behavior for both tiered and local segments. |
| **LocalLog** | `storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java` | An append-only log for storing messages locally. Manages a sequence of LogSegments. |
| **LogSegment** | `storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java` | A segment of the log. Each segment has FileRecords (the actual messages) and various indexes. |
| **FileRecords** | `clients/src/main/java/org/apache/kafka/common/record/FileRecords.java` | Manages message storage in local files using memory-mapped files. |
| **LogManager** | `storage/src/main/java/org/apache/kafka/storage/internals/log/LogManager.java` | Utility class for log management operations. |
| **LogSegments** | `storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegments.java` | Collection of LogSegment instances. |

### 1.2 Index Components

| Component | Description |
|-----------|-------------|
| **OffsetIndex** | Maps logical offsets to physical file positions |
| **TimeIndex** | Maps timestamps to offsets |
| **TransactionIndex** | Tracks aborted transactions |

### 1.3 Data Flow

```
Producer → UnifiedLog.appendAsLeader() → LocalLog.append() → LogSegment.append() → FileRecords.append()
                                                     ↓
                                               Update indexes (OffsetIndex, TimeIndex, TransactionIndex)
```

```
Consumer → UnifiedLog.read() → LocalLog.read() → LogSegment.read() → FileRecords.slice()
```

## 2. Pulsar Managed-Ledger Overview

Apache Pulsar's managed-ledger module provides an abstraction over BookKeeper that is conceptually similar to Kafka's log:

| Pulsar Component | Kafka Equivalent | Description |
|------------------|------------------|-------------|
| **ManagedLedger** | UnifiedLog/LocalLog | Append-only log with cursor management |
| **ManagedCursor** | Consumer offset tracking | Tracks read position |
| **LedgerHandle** | LogSegment | Individual BookKeeper ledger |
| **Entry** | RecordBatch | A single write unit |
| **ManagedLedgerFactory** | LogManager | Creates and manages ManagedLedgers |

### 2.1 Key Managed-Ledger Maven Coordinates

```xml
<dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>managed-ledger</artifactId>
    <version>${pulsar.version}</version>
</dependency>
```

## 3. Integration Approach

### 3.1 Option A: Create Storage Abstraction Layer (Recommended)

Create an abstract storage interface that both FileRecords-based storage and BookKeeper-based storage can implement:

#### Step 1: Define Storage Interface

```java
// storage/api/src/main/java/org/apache/kafka/storage/api/LogStorage.java
package org.apache.kafka.storage.api;

public interface LogStorage extends Closeable {
    /**
     * Append records to the storage.
     * @param records The records to append
     * @return The number of bytes appended
     */
    long append(MemoryRecords records) throws IOException;
    
    /**
     * Read records starting from the given offset.
     * @param startOffset The offset to start reading from
     * @param maxBytes Maximum bytes to read
     * @return FetchDataInfo containing the records
     */
    FetchDataInfo read(long startOffset, int maxBytes) throws IOException;
    
    /**
     * Get the size in bytes.
     */
    int sizeInBytes();
    
    /**
     * Get the base offset of this storage unit.
     */
    long baseOffset();
    
    /**
     * Flush data to durable storage.
     */
    void flush() throws IOException;
    
    /**
     * Truncate to the given offset.
     */
    int truncateTo(long offset) throws IOException;
}
```

#### Step 2: Implement BookKeeper Storage

```java
// storage/src/main/java/org/apache/kafka/storage/bookkeeper/BookKeeperLogStorage.java
package org.apache.kafka.storage.bookkeeper;

import org.apache.bookkeeper.client.api.LedgerHandle;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.managed.ledger.ManagedLedger;

public class BookKeeperLogStorage implements LogStorage {
    private final ManagedLedger managedLedger;
    private final long baseOffset;
    
    // Implementation details...
}
```

### 3.2 Components to Modify

#### 3.2.1 Primary Modification Points

| File | Changes Required |
|------|------------------|
| **LogSegment.java** | Replace `FileRecords` with `LogStorage` interface. This is the core change. |
| **LocalLog.java** | Update to work with abstract storage instead of file-based segments. |
| **UnifiedLog.java** | Minimal changes - delegates to LocalLog. |
| **LogLoader.java** | Add logic to load from BookKeeper instead of local files. |
| **LogConfig.java** | Add BookKeeper-specific configuration options. |

#### 3.2.2 Detailed Changes for LogSegment.java

Current implementation uses:
```java
private final FileRecords log;  // Line 79
```

Change to:
```java
private final LogStorage log;  // Abstract interface
```

Affected methods:
- `append(long largestOffset, MemoryRecords records)` - Lines 250-280
- `read(long startOffset, int maxSize, Optional<Long> maxPositionOpt, boolean minOneMessage)` - Lines 431-459
- `translateOffset(long offset, int startingFilePosition)` - Lines 394-397
- `recover(ProducerStateManager, LeaderEpochFileCache)` - Lines 478-524
- `truncateTo(long offset)` - Lines 557-588
- `flush()` - Lines 624-645
- `deleteIfExists()` - Lines 782-798

#### 3.2.3 Index Handling

BookKeeper doesn't natively support offset-based indexing like Kafka. You have two options:

**Option A: Keep local indexes**
- Store OffsetIndex, TimeIndex, TransactionIndex locally
- BookKeeper handles the actual record data

**Option B: Embed index in BookKeeper entries**
- Store index information as metadata in BookKeeper entries
- Build in-memory index on recovery

### 3.3 Configuration Changes

Add to `LogConfig.java`:

```java
// BookKeeper storage configuration
public static final String STORAGE_TYPE_CONFIG = "storage.type";
public static final String STORAGE_TYPE_DOC = "Storage type: 'file' (default) or 'bookkeeper'";

public static final String BOOKKEEPER_ENSEMBLE_SIZE_CONFIG = "bookkeeper.ensemble.size";
public static final String BOOKKEEPER_WRITE_QUORUM_CONFIG = "bookkeeper.write.quorum";
public static final String BOOKKEEPER_ACK_QUORUM_CONFIG = "bookkeeper.ack.quorum";
public static final String BOOKKEEPER_LEDGER_PATH_CONFIG = "bookkeeper.ledger.path";
public static final String MANAGED_LEDGER_MAX_ENTRIES_PER_LEDGER_CONFIG = "managed.ledger.max.entries.per.ledger";
```

## 4. Mapping Concepts

### 4.1 Offset Mapping

| Kafka Concept | BookKeeper/Managed-Ledger Concept |
|---------------|-----------------------------------|
| Kafka Offset | LedgerId:EntryId composite |
| Log End Offset | Last confirmed entry |
| Log Start Offset | First available entry |
| High Watermark | Confirmed entries across replicas |

### 4.2 Segment Mapping

| Kafka | BookKeeper |
|-------|------------|
| LogSegment | Single Ledger |
| Segment rolling | Ledger rolling |
| Segment deletion | Ledger deletion |
| Segment recovery | Ledger recovery |

### 4.3 Replication Model

**Key Difference:** BookKeeper handles replication internally, which differs from Kafka's leader-follower replication.

| Kafka Model | BookKeeper Model |
|-------------|------------------|
| Leader writes, followers replicate | All bookies are equal writers |
| ISR (In-Sync Replicas) | Write/Ack quorums |
| Leader election via controller | No leader - all writes go to quorum |

## 5. Implementation Steps

### Step 1: Create Storage API Module
```
storage/api/
├── src/main/java/org/apache/kafka/storage/api/
│   ├── LogStorage.java
│   ├── LogStorageFactory.java
│   └── StorageConfig.java
```

### Step 2: Create BookKeeper Storage Module
```
storage/bookkeeper/
├── build.gradle
└── src/main/java/org/apache/kafka/storage/bookkeeper/
    ├── BookKeeperLogStorage.java
    ├── BookKeeperLogSegment.java
    ├── ManagedLedgerAdapter.java
    └── BookKeeperStorageFactory.java
```

### Step 3: Modify Core Storage Classes
1. Update `LogSegment` to use `LogStorage` interface
2. Update `LocalLog` to support pluggable storage
3. Add factory pattern for storage creation
4. Update `LogLoader` for BookKeeper recovery

### Step 4: Handle Index Management
- Decide on local vs. distributed index strategy
- Implement offset-to-entry mapping
- Handle transaction index in BookKeeper context

### Step 5: Configuration and Integration
- Add BookKeeper configuration to Kafka config
- Create storage factory based on configuration
- Handle migration path for existing data

## 6. Challenges and Considerations

### 6.1 Offset Model Mismatch
Kafka uses contiguous 64-bit offsets, while BookKeeper uses (ledgerId, entryId) pairs.

**Solution:** Create an offset mapping layer:
```java
public class BookKeeperOffsetMapper {
    // Kafka offset = (ledgerId << 32) | entryId  (simplified)
    // Or use a dedicated mapping ledger
}
```

### 6.2 Read Path Optimization
BookKeeper's read pattern differs from Kafka's memory-mapped file approach.

**Solutions:**
- Use Pulsar's read cache
- Implement entry cache similar to Kafka's page cache
- Use BookKeeper's LAC (Last Add Confirmed) for efficient tailing

### 6.3 Transaction Support
Kafka's transaction model needs to work with BookKeeper's semantics.

**Considerations:**
- Transaction markers stored as special entries
- Aborted transaction tracking
- Producer state management

### 6.4 Compaction
Log compaction is more complex with BookKeeper.

**Approach:**
- Create new compacted ledger
- Delete old ledgers
- Update offset mappings

## 7. Existing Similar Projects

### 7.1 KoP (Kafka on Pulsar)
Apache Pulsar's KoP project implements Kafka protocol on top of Pulsar, which uses managed-ledger. This is an excellent reference.

Repository: https://github.com/streamnative/kop

### 7.2 Key Insights from KoP
- Uses Pulsar topic as Kafka topic mapping
- Implements Kafka offset to Pulsar MessageId mapping
- Handles Kafka transaction semantics

## 8. Recommended Approach

Based on the analysis, the recommended approach is:

1. **Start with KoP as reference** - Study how KoP maps Kafka concepts to Pulsar/BookKeeper
2. **Create abstraction layer** - Define `LogStorage` interface in Kafka
3. **Implement BookKeeper adapter** - Create `BookKeeperLogStorage` implementation
4. **Gradual migration** - Allow mixed mode with some topics on files, others on BookKeeper
5. **Test thoroughly** - Ensure all Kafka semantics are preserved

## 9. Files to Modify Summary

### Core Storage Layer
| Priority | File | Type of Change |
|----------|------|----------------|
| HIGH | `storage/src/main/java/.../LogSegment.java` | Major refactoring |
| HIGH | `storage/src/main/java/.../LocalLog.java` | Major refactoring |
| MEDIUM | `storage/src/main/java/.../UnifiedLog.java` | Minor updates |
| MEDIUM | `storage/src/main/java/.../LogLoader.java` | Add BookKeeper recovery |
| MEDIUM | `storage/src/main/java/.../LogConfig.java` | Add configurations |
| LOW | `storage/src/main/java/.../LogManager.java` | Factory updates |

### New Files to Create
| File | Description |
|------|-------------|
| `storage/api/src/.../LogStorage.java` | Storage abstraction interface |
| `storage/api/src/.../LogStorageFactory.java` | Factory for storage creation |
| `storage/bookkeeper/src/.../BookKeeperLogStorage.java` | BookKeeper implementation |
| `storage/bookkeeper/src/.../ManagedLedgerAdapter.java` | Pulsar ML adapter |
| `storage/bookkeeper/src/.../OffsetMapper.java` | Offset mapping utilities |

### Configuration Files
| File | Change |
|------|--------|
| `config/server.properties` | Add BookKeeper configs |
| `settings.gradle` | Add new modules |
| `build.gradle` (root) | Add dependencies |

## 10. Dependencies to Add

```gradle
// In storage/bookkeeper/build.gradle
// Define versions in root build.gradle or gradle.properties:
// pulsarVersion = '3.2.0'
// bookkeeperVersion = '4.16.4'
// zookeeperVersion = '3.8.4'

dependencies {
    implementation "org.apache.pulsar:managed-ledger:3.2.0"         // or ${pulsarVersion}
    implementation "org.apache.bookkeeper:bookkeeper-server:4.16.4" // or ${bookkeeperVersion}
    implementation "org.apache.zookeeper:zookeeper:3.8.4"           // or ${zookeeperVersion}
}
```

**Note:** Ensure compatibility between Pulsar managed-ledger and BookKeeper versions. The managed-ledger module from Pulsar typically bundles compatible BookKeeper dependencies.

## 11. Conclusion

Replacing Kafka's storage with BookKeeper is a significant undertaking that requires:

1. **Creating an abstraction layer** for pluggable storage
2. **Implementing BookKeeper adapter** using Pulsar's managed-ledger
3. **Handling concept mismatches** (offsets, replication, transactions)
4. **Thorough testing** to preserve Kafka semantics

The recommended starting point is studying the KoP (Kafka on Pulsar) project, which has already solved many of these challenges. This document provides the roadmap for where to make changes in the Kafka codebase.

---

*Document created for Kafka storage architecture analysis*
*Reference commit: Kafka trunk*
