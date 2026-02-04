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

Partition.scala 非私有方法详解

状态查询方法

| 方法                                | 行号   | 机制       | 作用                             |
  |-----------------------------------|------|----------|--------------------------------|
| unifiedLog()                      | 227  | 日志管理     | 返回分区日志的 Optional 包装            |
| hasLateTransaction(currentTimeMs) | 229  | 事务机制     | 检查分区是否存在超时未完成的事务               |
| isUnderReplicated                 | 231  | 副本管理     | 判断副本数是否不足（ISR < AR）            |
| isUnderMinIsr                     | 236  | ISR管理    | 判断ISR大小是否低于最小ISR配置             |
| isAtMinIsr                        | 253  | ISR管理    | 判断ISR大小是否等于有效最小ISR             |
| isReassigning                     | 255  | 副本重分配    | 判断分区是否正在进行副本重分配                |
| isAddingLocalReplica              | 257  | 副本重分配    | 判断本地broker是否正在被添加为副本           |
| isAddingReplica(replicaId)        | 259  | 副本重分配    | 判断指定副本是否正在被添加                  |
| producerIdCount                   | 261  | 事务机制     | 返回活跃的生产者ID数量                   |
| inSyncReplicaIds                  | 266  | ISR管理    | 返回当前ISR中所有副本ID的集合              |
| isLeader                          | 424  | Leader选举 | 判断当前broker是否是分区leader          |
| leaderIdIfLocal                   | 426  | Leader选举 | 如果是leader则返回leader ID，否则返回None |
| getLeaderEpoch                    | 583  | Leader选举 | 返回当前leader epoch               |
| getPartitionEpoch                 | 585  | 元数据管理    | 返回分区epoch（KRaft模式）             |
| logStartOffset                    | 1542 | 日志管理     | 返回日志起始偏移量                      |

  ---
日志管理方法

| 方法                            | 行号  | 机制     | 作用                            |
  |-------------------------------|-----|--------|-------------------------------|
| localLogOrException           | 407 | 日志管理   | 获取本地日志，不存在则抛出异常               |
| futureLocalLogOrException     | 412 | 日志管理   | 获取未来日志（用于日志目录迁移），不存在则抛异常      |
| leaderLogIfLocal              | 417 | 日志管理   | 如果是leader则返回本地日志              |
| localLogWithEpochOrThrow(...) | 430 | 日志管理   | 验证leader epoch并获取本地日志         |
| setLog(log, isFutureLog)      | 444 | 日志管理   | 设置分区日志（测试用）                   |
| topicId                       | 458 | 元数据管理  | 获取Topic ID                    |
| maybeCreateFutureReplica(...) | 296 | 日志目录迁移 | 创建未来副本（用于ReplicaAlterLogDirs） |
| createLogIfNotExists(...)     | 325 | 日志管理   | 创建日志（如果不存在）                   |
| logDirectoryId()              | 532 | 日志管理   | 获取当前日志目录ID                    |
| futureReplicaDirectoryId()    | 531 | 日志管理   | 获取未来日志目录ID                    |

  ---
副本管理方法

| 方法                                         | 行号  | 机制         | 作用            |
  |--------------------------------------------|-----|------------|---------------|
| getReplica(replicaId)                      | 371 | 副本管理       | 获取指定远程副本对象    |
| remoteReplicas                             | 466 | 副本管理       | 返回所有远程副本的迭代器  |
| updateAssignmentAndIsr(...)                | 843 | 副本管理/ISR管理 | 更新副本分配和ISR集合  |
| removeFutureLocalReplica(deleteFromLogDir) | 475 | 日志目录迁移     | 移除未来本地副本      |
| futureReplicaDirChanged(newDestinationDir) | 469 | 日志目录迁移     | 检查未来副本目录是否已改变 |

  ---
Leader/Follower切换方法

| 方法                                  | 行号  | 机制       | 作用                          |
  |-------------------------------------|-----|----------|-----------------------------|
| makeLeader(...)                     | 592 | Leader选举 | 将本地副本切换为leader，初始化ISR、副本状态  |
| makeFollower(...)                   | 698 | Leader选举 | 将本地副本切换为follower，更新leader信息 |
| invokeOnBecomingFollowerListeners() | 566 | Leader选举 | 通知监听器分区已切换为follower         |

  ---
HW（高水位）管理方法

| 方法                                             | 行号   | 机制        | 作用                             |
  |------------------------------------------------|------|-----------|--------------------------------|
| checkEnoughReplicasReachOffset(requiredOffset) | 965  | HW管理/生产确认 | 检查是否有足够的副本追上指定偏移量（用于all ack）   |
| lowWatermarkIfLeader                           | 1082 | LW管理      | 计算低水位（所有存活副本的最小logStartOffset） |

  ---
ISR动态调整方法

| 方法                             | 行号   | 机制               | 作用                           |
  |--------------------------------|------|------------------|------------------------------|
| updateFollowerFetchState(...)  | 771  | ISR管理/Follower跟踪 | 更新follower的拉取状态，触发ISR扩展和HW更新 |
| getOutOfSyncReplicas(maxLagMs) | 1186 | ISR收缩            | 获取所有不同步的副本集合                 |
| maybeShrinkIsr()               | 1111 | ISR收缩            | 检查并收缩ISR（移除滞后副本）             |

  ---
数据追加方法

| 方法                                          | 行号   | 机制    | 作用                  |
  |---------------------------------------------|------|-------|---------------------|
| appendRecordsToLeader(...)                  | 1253 | 生产者机制 | 将记录追加到leader副本      |
| appendRecordsToFollowerOrFutureReplica(...) | 1223 | 副本同步  | 将记录追加到follower或未来副本 |
| maybeStartTransactionVerification(...)      | 485  | 事务机制  | 启动事务验证（用于幂等性/事务性生产） |
| removeExpiredProducers(currentTimeMs)       | 264  | 事务机制  | 移除过期的生产者状态          |

  ---
数据读取方法

| 方法                            | 行号   | 机制       | 作用                                |
  |-------------------------------|------|----------|-----------------------------------|
| fetchRecords(...)             | 1312 | 消费者拉取    | 从分区读取记录（处理follower和消费者拉取）         |
| fetchOffsetForTimestamp(...)  | 1467 | Offset查询 | 根据时间戳查找对应的offset                  |
| fetchOffsetSnapshot(...)      | 1535 | Offset查询 | 获取offset快照（包含HW、LEO等）             |
| lastOffsetForLeaderEpoch(...) | 1621 | 日志截断/恢复  | 查找指定leader epoch的结束offset         |
| activeProducerState           | 1518 | 事务机制     | 返回活跃的生产者状态（用于DescribeProducers请求） |

  ---
日志截断/删除方法

| 方法                            | 行号   | 机制   | 作用                    |
  |-------------------------------|------|------|-----------------------|
| deleteRecordsOnLeader(offset) | 1554 | 日志删除 | 在leader上删除记录到指定offset |
| truncateTo(offset, isFuture)  | 1583 | 日志截断 | 截断日志到指定offset         |
| truncateFullyAndStartAt(...)  | 1598 | 日志截断 | 完全清空日志并从新offset开始     |
| delete()                      | 537  | 分区删除 | 删除分区，清除所有状态           |
| markOffline()                 | 552  | 故障处理 | 标记分区为离线状态             |

  ---
副本替换方法（日志目录迁移）

| 方法                                           | 行号  | 机制     | 作用                |
  |----------------------------------------------|-----|--------|-------------------|
| maybeReplaceCurrentWithFutureReplica()       | 495 | 日志目录迁移 | 检查并用未来副本替换当前副本    |
| runCallbackIfFutureReplicaCaughtUp(callback) | 504 | 日志目录迁移 | 如果未来副本追上当前副本，执行回调 |

  ---
监听器管理方法

| 方法                         | 行号  | 机制   | 作用      |
  |----------------------------|-----|------|---------|
| maybeAddListener(listener) | 268 | 事件通知 | 添加分区监听器 |
| removeListener(listener)   | 284 | 事件通知 | 移除分区监听器 |

  ---
延迟操作方法

| 方法                           | 行号   | 机制   | 作用                      |
  |------------------------------|------|------|-------------------------|
| tryCompleteDelayedRequests() | 1107 | 延迟操作 | 尝试完成所有等待的延迟操作（生产/拉取/删除） |

  ---
核心机制总结

| 机制       | 核心方法数 | 说明                  |
  |----------|-------|---------------------|
| ISR管理    | 8     | 动态扩展/收缩ISR，HW更新     |
| Leader选举 | 4     | leader/follower角色切换 |
| 日志管理     | 12    | 日志创建、读取、截断、目录迁移     |
| 副本管理     | 6     | 副本状态跟踪、分配管理         |
| 事务机制     | 4     | 事务验证、生产者状态管理        |
| HW/LW管理  | 2     | 高水位和低水位计算           |
| 副本同步     | 2     | follower数据拉取和追加     |

这个文件是 Kafka 分区管理的核心，协调了副本同步、HW管理、ISR动态调整等关键机制。