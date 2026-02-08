# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# AsyncCoordinatorRuntime 继承方案实现计划

## 目标
创建 `AsyncCoordinatorRuntime` 类继承 `CoordinatorRuntime`，将异步实现隔离到子类中，保持原有实现不变。

## 可行性分析

### ✅ 可行的关键点
1. `maybeCreateContext(TopicPartition tp)` 是 **package-private**，可以被子类覆盖
2. `CoordinatorContext` 是非 static 内部类，可以被继承
3. 使用 `CoordinatorContext::new` 创建实例，可以在子类中替换

### ⚠️ 需要修改的点
1. `CoordinatorContext` 构造函数是 `private`，需要改为 `protected`
2. 一些 `private` 方法需要改为 `protected`，以便子类覆盖

## 实现步骤

### 第一步：修改 CoordinatorRuntime（最小化修改）

#### 1.1 修改 CoordinatorContext 构造函数可见性
```java
// 从 private 改为 protected
protected CoordinatorContext(TopicPartition tp) {
    // ... 现有代码
}
```

#### 1.2 修改需要被覆盖的方法可见性
需要将以下方法从 `private` 改为 `protected`：
- `append()`
- `flushCurrentBatch()`
- `maybeFlushCurrentBatch()`
- `completeTransaction()`
- `maybeAllocateNewBatch()`
- `freeCurrentBatch()`
- `failCurrentBatch()`

**理由**：这些方法需要在 `AsyncCoordinatorContext` 中被覆盖。

#### 1.3 回退异步相关的修改
- 移除 `RecordToReplay` 类
- 移除 `CoordinatorBatch.recordsToReplay` 字段
- 移除 `inFlightBatch` 字段
- 移除 `asyncOperationInProgress` 字段
- 移除 `handleAsyncFlushCompletion()` 方法
- 移除 `handleTransactionMarkerCompletion()` 方法
- 移除 `freeInFlightBatch()` 方法
- 恢复 `append()` 的原始实现（立即 replay）
- 恢复 `flushCurrentBatch()` 的原始实现（同步写入）
- 恢复 `completeTransaction()` 的原始实现（同步写入）
- 恢复 `unload()` 的原始实现
- 恢复 `transitionTo()` 中的 HighWatermarkListener 注册

### 第二步：创建 AsyncCoordinatorRuntime

#### 2.1 创建类结构
```java
package org.apache.kafka.coordinator.common.runtime;

public class AsyncCoordinatorRuntime<S extends CoordinatorShard<U>, U>
    extends CoordinatorRuntime<S, U> {

    // 使用父类的构造函数
    private AsyncCoordinatorRuntime(...) {
        super(...);
    }

    // Builder 模式
    public static class Builder<S extends CoordinatorShard<U>, U>
        extends CoordinatorRuntime.Builder<S, U> {
        // ...
    }

    // 覆盖 maybeCreateContext
    @Override
    CoordinatorContext maybeCreateContext(TopicPartition tp) {
        return coordinators.computeIfAbsent(tp, AsyncCoordinatorContext::new);
    }

    // 内部类：AsyncCoordinatorContext
    class AsyncCoordinatorContext extends CoordinatorContext {
        // 异步实现
    }
}
```

#### 2.2 实现 AsyncCoordinatorContext

##### 2.2.1 添加异步相关字段
```java
class AsyncCoordinatorContext extends CoordinatorContext {
    /**
     * The batch currently being written asynchronously.
     */
    CoordinatorBatch<U> inFlightBatch;

    /**
     * Flag to prevent concurrent async operations.
     */
    boolean asyncOperationInProgress;

    // 构造函数
    AsyncCoordinatorContext(TopicPartition tp) {
        super(tp);
    }
}
```

##### 2.2.2 添加 RecordToReplay 类
```java
private static class RecordToReplay<U> {
    final long offset;
    final long producerId;
    final short producerEpoch;
    final U record;
    final SimpleRecord simpleRecord;
    final boolean shouldReplay;

    RecordToReplay(...) {
        // ...
    }
}
```

##### 2.2.3 修改 CoordinatorBatch（在 AsyncCoordinatorContext 中）
需要创建一个新的 `AsyncCoordinatorBatch` 类，或者修改现有的 `CoordinatorBatch`。

**问题**：`CoordinatorBatch` 是 `CoordinatorRuntime` 的 static 内部类，无法在子类中修改。

**解决方案**：
- 方案 A：在 `CoordinatorRuntime` 中将 `CoordinatorBatch` 泛型化（添加 `recordsToReplay` 字段，但在同步版本中不使用）
- 方案 B：在 `AsyncCoordinatorRuntime` 中创建新的 `AsyncCoordinatorBatch` 类

**推荐方案 A**：因为 `recordsToReplay` 字段不会影响同步版本的性能（只是一个空列表）。

##### 2.2.4 覆盖关键方法
```java
@Override
protected void append(...) {
    // 异步实现：创建 RecordToReplay，不立即 replay
}

@Override
protected void flushCurrentBatch() {
    // 异步实现：使用 appendAsync
}

@Override
protected void completeTransaction(...) {
    // 异步实现：使用 appendAsync
}

@Override
protected void maybeAllocateNewBatch(...) {
    // 检查 asyncOperationInProgress
}

@Override
protected void unload() {
    // 清理 inFlightBatch
    super.unload();
}
```

##### 2.2.5 添加异步完成处理方法
```java
private void handleAsyncFlushCompletion(...) {
    // 异步完成处理逻辑
}

private void handleTransactionMarkerCompletion(...) {
    // 事务标记完成处理逻辑
}

private void freeInFlightBatch() {
    // 清理 inFlightBatch
}
```

### 第三步：处理 CoordinatorBatch 泛型化

#### 3.1 在 CoordinatorRuntime 中修改 CoordinatorBatch
```java
private static class CoordinatorBatch<U> {
    // ... 现有字段

    /**
     * The list of records to replay (used by AsyncCoordinatorRuntime).
     * Empty in synchronous mode.
     */
    final List<RecordToReplay<U>> recordsToReplay;

    CoordinatorBatch(...) {
        // ...
        this.recordsToReplay = new ArrayList<>();
    }
}
```

**问题**：`RecordToReplay` 类在哪里定义？

**解决方案**：
- 方案 A：在 `CoordinatorRuntime` 中定义 `RecordToReplay`（但同步版本不使用）
- 方案 B：使用 `Object` 类型，在 `AsyncCoordinatorRuntime` 中强制转换

**推荐方案 A**：类型安全。

### 第四步：测试

#### 4.1 单元测试
- 保持现有的 `CoordinatorRuntimeTest.java`（测试同步版本）
- 保持 `CoordinatorRuntimeAsyncTest.java`（测试异步版本，使用 `AsyncCoordinatorRuntime`）

#### 4.2 集成测试
- 默认使用 `CoordinatorRuntime`（同步版本）
- 可以通过配置切换到 `AsyncCoordinatorRuntime`

## 挑战和风险

### 挑战 1：CoordinatorBatch 泛型化
`CoordinatorBatch` 是 static 内部类，需要添加 `recordsToReplay` 字段。这会影响同步版本，但影响很小（只是一个空列表）。

### 挑战 2：方法可见性
需要将多个 `private` 方法改为 `protected`，这会增加 API 表面积。

### 挑战 3：RecordToReplay 位置
`RecordToReplay` 类需要在 `CoordinatorRuntime` 中定义，但只在异步版本中使用。

### 挑战 4：代码重复
`AsyncCoordinatorContext` 需要覆盖多个方法，可能导致代码重复。

## 替代方案对比

| 方案 | 优点 | 缺点 |
|------|------|------|
| 继承方案 | 向后兼容，逐步迁移 | 需要修改父类可见性，CoordinatorBatch 泛型化 |
| 配置标志位 | 代码集中，易维护 | 单一类变复杂 |
| 直接修改 | 最简单 | 无法回退 |

## 最终建议

### ⚠️ 继承方案的复杂度评估

经过详细分析，继承方案需要：
1. 修改 `CoordinatorContext` 构造函数可见性
2. 修改 7+ 个方法的可见性（private → protected）
3. 在父类中添加 `RecordToReplay` 类（虽然同步版本不使用）
4. 在父类中修改 `CoordinatorBatch`（添加 `recordsToReplay` 字段）
5. 处理方法覆盖的复杂性

### 💡 修正后的建议

考虑到复杂度，我建议：

**方案 1：配置标志位（推荐）** ⭐
- 在 `CoordinatorRuntime` 中添加 `boolean useAsyncWrite` 配置
- 在关键方法中根据标志选择同步或异步行为
- 优点：代码集中，易于维护，测试简单
- 缺点：单一类稍微复杂

**方案 2：继承方案（如果必须隔离）**
- 按照上述计划实现
- 优点：完全隔离，向后兼容
- 缺点：需要修改父类，增加复杂度

**方案 3：保持当前实现（最简单）**
- 直接使用异步实现
- 优点：最简单，性能最好
- 缺点：无法回退到同步版本

## 您的选择

请告诉我您更倾向于哪个方案：
1. 继承方案（需要修改父类可见性）
2. 配置标志位方案
3. 保持当前异步实现

如果选择继承方案，我可以立即开始实现。
