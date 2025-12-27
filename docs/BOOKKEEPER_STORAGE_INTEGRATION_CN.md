# Kafka存储层替换为BookKeeper指南

## 概述

本文档详细说明如何将Kafka底层的存储替换为BookKeeper，复用Apache Pulsar的managed-ledger模块。

## 1. Kafka存储架构核心组件

### 1.1 需要修改的核心类

| 优先级 | 类文件 | 路径 | 修改说明 |
|--------|--------|------|----------|
| **高** | `LogSegment.java` | `storage/src/main/java/org/apache/kafka/storage/internals/log/` | 替换`FileRecords`为抽象存储接口，这是最核心的修改点 |
| **高** | `LocalLog.java` | `storage/src/main/java/org/apache/kafka/storage/internals/log/` | 管理LogSegment的集合，需要支持插件化存储 |
| **中** | `UnifiedLog.java` | `storage/src/main/java/org/apache/kafka/storage/internals/log/` | 统一日志视图，变化较小 |
| **中** | `LogLoader.java` | `storage/src/main/java/org/apache/kafka/storage/internals/log/` | 添加BookKeeper恢复逻辑 |
| **中** | `LogConfig.java` | `storage/src/main/java/org/apache/kafka/storage/internals/log/` | 添加BookKeeper配置项 |
| **低** | `FileRecords.java` | `clients/src/main/java/org/apache/kafka/common/record/` | 理解现有实现，作为参考 |

### 1.2 索引相关类

| 类文件 | 说明 |
|--------|------|
| `OffsetIndex.java` | 偏移量索引，映射逻辑偏移到物理位置 |
| `TimeIndex.java` | 时间索引，映射时间戳到偏移量 |
| `TransactionIndex.java` | 事务索引，跟踪已中止的事务 |

## 2. Kafka与Pulsar Managed-Ledger概念映射

| Kafka概念 | Pulsar Managed-Ledger概念 | 说明 |
|-----------|---------------------------|------|
| `UnifiedLog` / `LocalLog` | `ManagedLedger` | 可追加的日志 |
| `LogSegment` | `Ledger` (单个账本) | 存储单元 |
| `FileRecords` | `LedgerHandle` | 数据读写句柄 |
| `RecordBatch` | `Entry` | 单次写入单元 |
| Consumer Offset | `ManagedCursor` | 消费位置跟踪 |
| `LogManager` | `ManagedLedgerFactory` | 创建和管理日志 |

## 3. 推荐实施方案

### 3.1 创建抽象存储接口

在 `storage/api` 模块创建新接口:

```java
// storage/api/src/main/java/org/apache/kafka/storage/api/LogStorage.java
package org.apache.kafka.storage.api;

public interface LogStorage extends Closeable {
    // 追加记录
    long append(MemoryRecords records) throws IOException;
    
    // 读取记录
    FetchDataInfo read(long startOffset, int maxBytes) throws IOException;
    
    // 获取大小
    int sizeInBytes();
    
    // 获取基础偏移量
    long baseOffset();
    
    // 刷盘
    void flush() throws IOException;
    
    // 截断到指定偏移量
    int truncateTo(long offset) throws IOException;
}
```

### 3.2 实现BookKeeper存储

创建新模块 `storage/bookkeeper`:

```
storage/bookkeeper/
├── build.gradle
└── src/main/java/org/apache/kafka/storage/bookkeeper/
    ├── BookKeeperLogStorage.java      # 实现LogStorage接口
    ├── BookKeeperLogSegment.java      # 对应LogSegment
    ├── ManagedLedgerAdapter.java      # 封装Pulsar ML
    ├── BookKeeperStorageFactory.java  # 工厂类
    └── OffsetMapper.java              # 偏移量映射
```

### 3.3 修改LogSegment.java

**当前实现 (第79行):**
```java
private final FileRecords log;
```

**修改为:**
```java
private final LogStorage log;
```

**需要修改的方法:**
- `append()` - 第250-280行
- `read()` - 第431-459行
- `translateOffset()` - 第394-397行
- `recover()` - 第478-524行
- `truncateTo()` - 第557-588行
- `flush()` - 第624-645行

## 4. 关键技术挑战

### 4.1 偏移量模型不匹配

**问题:** Kafka使用连续的64位偏移量，而BookKeeper使用(ledgerId, entryId)元组。

**解决方案:**
```java
public class OffsetMapper {
    // 方案1: 组合映射
    // Kafka offset = (ledgerId << 32) | entryId
    
    // 方案2: 使用独立的映射账本存储偏移量到(ledgerId, entryId)的映射
}
```

### 4.2 复制模型差异

| Kafka模型 | BookKeeper模型 |
|-----------|----------------|
| Leader写入，Follower复制 | 写入法定数量的Bookie |
| ISR机制 | Write/Ack Quorum |
| 需要Leader选举 | 无Leader概念 |

**建议:** 可以利用BookKeeper的内置复制，减少Kafka层的复制逻辑。

### 4.3 事务支持

需要确保Kafka的事务语义在BookKeeper上正确实现:
- 事务标记作为特殊Entry存储
- 已中止事务的跟踪
- 生产者状态管理

## 5. 参考项目

### KoP (Kafka on Pulsar)

Apache Pulsar的KoP项目已经实现了在Pulsar/BookKeeper上运行Kafka协议:

**仓库地址:** https://github.com/streamnative/kop

**可参考的实现:**
- Kafka偏移量到Pulsar MessageId的映射
- Kafka事务语义的实现
- 协议兼容性处理

## 6. 依赖配置

### 6.1 Gradle依赖

```gradle
// storage/bookkeeper/build.gradle
// 在根 build.gradle 或 gradle.properties 中定义版本:
// pulsarVersion = '3.2.0'
// bookkeeperVersion = '4.16.4'

dependencies {
    implementation "org.apache.pulsar:managed-ledger:3.2.0"         // 或 ${pulsarVersion}
    implementation "org.apache.bookkeeper:bookkeeper-server:4.16.4" // 或 ${bookkeeperVersion}
}
```

**注意:** 确保Pulsar managed-ledger与BookKeeper版本兼容。Pulsar的managed-ledger模块通常会包含兼容的BookKeeper依赖。

### 6.2 配置项

需要在 `server.properties` 添加:

```properties
# 存储类型
storage.type=bookkeeper  # 或 'file' (默认)

# BookKeeper配置
bookkeeper.ensemble.size=3
bookkeeper.write.quorum=2
bookkeeper.ack.quorum=2
bookkeeper.zk.servers=localhost:2181
managed.ledger.max.entries.per.ledger=50000
```

## 7. 实施步骤总结

1. **研究KoP项目** - 学习现有实现
2. **创建抽象层** - 定义`LogStorage`接口
3. **实现适配器** - 创建BookKeeper存储实现
4. **修改核心类** - 主要是`LogSegment`和`LocalLog`
5. **处理索引** - 选择本地索引或分布式索引策略
6. **添加配置** - 支持切换存储类型
7. **测试验证** - 确保Kafka语义正确

## 8. 文件清单

### 需要修改的文件

```
storage/src/main/java/org/apache/kafka/storage/internals/log/
├── LogSegment.java          # 主要修改
├── LocalLog.java            # 主要修改
├── UnifiedLog.java          # 次要修改
├── LogLoader.java           # 添加BK恢复
├── LogConfig.java           # 添加配置
└── LogManager.java          # 工厂更新
```

### 需要新建的文件

```
storage/api/src/main/java/org/apache/kafka/storage/api/
├── LogStorage.java          # 存储接口
└── LogStorageFactory.java   # 工厂接口

storage/bookkeeper/src/main/java/org/apache/kafka/storage/bookkeeper/
├── BookKeeperLogStorage.java
├── BookKeeperLogSegment.java
├── ManagedLedgerAdapter.java
├── BookKeeperStorageFactory.java
└── OffsetMapper.java
```

---

*文档版本: 1.0*
*参考Kafka版本: trunk*
