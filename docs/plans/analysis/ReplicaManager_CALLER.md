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

⏺ ReplicaManager.scala 非private方法外部调用分析

根据分析，以下是 ReplicaManager.scala 中被其他类调用的非private方法及其作用：

  ---
一、生命周期管理方法

1. startup(): Unit (行331)

    - 外部调用者:
        - BrokerMetadataPublisher.scala:358
        - BrokerServer.scala (通过 lifecycle 管理)
    - 作用: 启动 ReplicaManager 的后台任务
        - 启动 ISR 过期检查线程
        - 启动日志目录失败处理器
        - 启动事务协调器相关的管理器
        - 初始化远程日志管理器

2. shutdown(checkpointHW: Boolean = true): Unit (行2268)

    - 外部调用者: BrokerServer.scala:878
    - 作用: 优雅关闭 ReplicaManager
        - 移除指标
        - 关闭日志目录失败处理器
        - 关闭副本获取管理器
        - 关闭副本日志目录变更管理器
        - 可选地检查点高水位标记

3. beginControlledShutdown(): Unit (行2263)

    - 外部调用者: BrokerServer.scala:812
    - 作用: 标记开始受控关闭过程，设置 isInControlledShutdown 标志

4. startHighWatermarkCheckPointThread(): Unit (行312)

    - 外部调用者: BrokerMetadataPublisher.scala:390
    - 作用: 启动高水位定期检查点线程

      ---

二、分区访问方法

5. getPartition(topicPartition: TopicPartition): HostedPartition (行488)

    - 外部调用者: Partition.scala (内部使用)
    - 作用: 获取分区的宿主状态
        - 返回状态
    - 行为: 检查分区是否在线、离线或不存在

6. getPartitionOrException(topicPartition: TopicPartition): Partition (行531)

    - 外部调用者: DelayedFetch.scala:75, LocalLeaderEndPoint.scala:118/125/132/139/174
    - 作用: 获取分区对象，失败时抛出异常
        - 如果分区离线，抛出 KafkaStorageException
        - 如果分区不存在，抛出相应错误异常

7. getPartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition] (行564)

    - 外部调用者: KafkaApis.scala:371, DelayedProduce.scala:95
    - 作用: 获取分区对象，返回 Either 类型
        - 返回 Right[Partition] 表示成功
        - 返回 Left[Errors] 表示错误类型

8. onlinePartition(topicPartition: TopicPartition): Option[Partition] (行511)

    - 外部调用者: Partition.scala (内部使用)
    - 作用: 获取在线的分区对象，返回 Option[Partition]

      ---

三、日志访问方法

9. getLog(topicPartition: TopicPartition): Option[UnifiedLog] (行329)

    - 外部调用者: TransactionStateManager.scala:453
    - 作用: 获取分区对应的日志对象

10. localLogOrException(topicPartition: TopicPartition): UnifiedLog (行584)

    - 外部调用者: LocalLeaderEndPoint.scala:185
    - 作用: 获取本地日志，失败时抛出异常

11. futureLocalLogOrException(topicPartition: TopicPartition): UnifiedLog (行588)

    - 外部调用者: LocalLeaderEndPoint.scala:238
    - 作用: 获取未来日志（用于副本迁移），失败时抛出异常

12. getLogConfig(topicPartition: TopicPartition): Option[LogConfig] (行2072)

    - 外部调用者: CoordinatorPartitionWriter.scala:98
    - 作用: 获取日志配置

13. getLogEndOffset(topicPartition: TopicPartition): Option[Long] (行2165)

    - 外部调用者: TransactionStateManager.scala:449
    - 作用: 获取日志末端偏移量

      ---

四、数据写入方法

14. appendRecordsToLeader(...): Map[TopicIdPartition, LogAppendResult] (行630)

    - 外部调用者:
        - CoordinatorPartitionWriter.scala:145
    - 作用: 将记录追加到 leader 副本，不等待复制确认
        - 支持事务验证
        - 追加到本地日志
        - 添加到延迟操作炼狱

15. appendRecordsToLeaderAsync(...): Map[TopicIdPartition, CompletableFuture[LogAppendResult]] (行657)

    - 外部调用者:
        - CoordinatorPartitionWriter.scala:176
    - 作用: 异步追加记录到 leader 副本，返回 CompletableFuture

16. appendRecords(...): Unit (行699)

    - 外部调用者: KafkaApis.scala:1825
    - 作用: 追加记录并等待复制确认
        - 验证 requiredAcks
        - 追加到 leader
        - 可能创建延迟 Produce 请求等待复制完成

17. handleProduceAppend(...): Unit (行758)

    - 外部调用者: KafkaApis.scala
    - 作用: 处理 Produce 请求，支持事务验证
        - 检查事务性生产者
        - 可能启动事务验证流程
        - 处理验证回调

      ---

五、数据读取方法

18. fetchMessages(...): Unit (行1706)

    - 外部调用者:
        - KafkaApis.scala:761
        - LocalLeaderEndPoint.scala:104
    - 作用: 从副本获取消息
        - 从本地日志读取
        - 处理远程存储获取
        - 可能创建延迟 Fetch 请求
        - 支持消费者和 follower 获取

19. readFromLog(...): Seq[(TopicIdPartition, LogReadResult)] (行1806)

    - 外部调用者: DelayedFetch.scala:161
    - 作用: 从多个分区读取日志数据
        - 处理 leader 节流
        - 查找首选读取副本
        - 返回读取结果

20. fetchOffset(...): Unit (行1492)

    - 外部调用者: KafkaApis.scala:810
    - 作用: 获取指定时间戳的偏移量
        - 支持 ListOffsets 请求
        - 处理远程存储的时间戳查找
        - 可能创建延迟远程 ListOffsets 操作

21. fetchOffsetForTimestamp(...): EpochOffset (行1618)

    - 外部调用者: fetchOffset 内部调用
    - 作用: 获取特定时间戳对应的偏移量

      ---

六、数据删除方法

22. deleteRecords(...): Unit (行1330)

    - 外部调用者:
        - KafkaApis.scala:1588
        - CoordinatorPartitionWriter.scala:203
    - 作用: 删除记录到指定偏移量
        - 在本地日志上删除
        - 可能创建延迟删除操作等待低水位标记到达

      ---

七、副本管理方法

23. alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] (行1169)

    - 外部调用者: KafkaApis.scala:2211
    - 作用: 修改副本日志目录
        - 移动分区日志到指定目录
        - 创建未来副本
        - 启动 ReplicaAlterDirThread 进行数据迁移

24. describeLogDirs(partitions: Set[TopicPartition]): List[DescribeLogDirsResult] (行1256)

    - 外部调用者: KafkaApis.scala:2242
    - 作用: 描述日志目录信息
        - 返回每个日志目录的详细信息
        - 包括分区大小、偏移量滞后等
        - 返回磁盘空间信息

25. isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean (行492)

    - 外部调用者: DelayedFetch.scala:170, fetchMessages:1749
    - 作用: 检查副本是否正在添加中（用于副本重分配场景）

      ---

八、事务协调方法

26. maybeSendPartitionToTransactionCoordinator(...): Unit (行1000)

    - 外部调用者: CoordinatorPartitionWriter.scala:115
    - 作用: 可能向事务协调器发送分区验证请求
        - 处理事务验证
        - 添加分区到事务

      ---

九、元数据方法

27. topicIdPartition(topicPartition: TopicPartition): TopicIdPartition (行483)

    - 外部调用者:
        - CoordinatorPartitionWriter.scala:144/175
        - TransactionStateManager.scala:296/670/809
    - 作用: 创建带 Topic ID 的 TopicPartition 对象

28. applyDelta(delta: TopicsDelta, newImage: MetadataImage): Unit (行2413)

    - 外部调用者: BrokerMetadataPublisher.scala:148
    - 作用: 应用 KRaft 元数据变更
        - 处理删除的分区
        - 处理新的 leader/follower 变更
        - 更新目录分配
        - 触发副本获取器启动

      ---

十、工具方法

29. maybeAddListener(partition: TopicPartition, listener: PartitionListener): Boolean (行403)

    - 外部调用者: CoordinatorPartitionWriter.scala:81
    - 作用: 注册分区监听器

30. removeListener(partition: TopicPartition, listener: PartitionListener): Unit (行415)

    - 外部调用者: CoordinatorPartitionWriter.scala:91
    - 作用: 移除分区监听器

31. shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean (行2072)

    - 外部调用者: DelayedFetch.scala:97/102, readFromLog:1814
    - 作用: 判断是否应该对 follower 进行节流

32. lastOffsetForLeaderEpoch(...): Seq[OffsetForLeaderTopicResult] (行2313)

    - 外部调用者: KafkaApis.scala
    - 作用: 获取指定 leader epoch 的最后偏移量，用于副本截断和恢复

33. activeProducerState(requestPartition: TopicPartition): PartitionResponse (行2355)

    - 外部调用者: KafkaApis.scala
    - 作用: 获取分区的活跃生产者状态，用于 DescribeProducers 请求

34. handleLogDirFailure(dir: String, notifyController: Boolean): Unit (行2212)

    - 外部调用者: LogManager.scala
    - 作用: 处理日志目录失败
        - 停止相关分区的服务
        - 通知控制器
        - 如果是元数据日志目录失败，则关闭 broker

35. markPartitionOffline(tp: TopicPartition): Unit (行2196)

    - 外部调用者: applyDelta 内部调用, handleLogDirFailure
    - 作用: 将分区标记为离线状态

      ---

主要外部调用者总结

| 调用者                        | 主要调用的方法                                                                                                                                                                        |
  |----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| KafkaApis                  | fetchMessages, fetchOffset, deleteRecords, appendRecords, alterReplicaLogDirs, describeLogDirs                                                                                 |
| BrokerServer               | startup, shutdown, beginControlledShutdown                                                                                                                                     |
| CoordinatorPartitionWriter | appendRecordsToLeader, appendRecordsToLeaderAsync, deleteRecords, maybeAddListener, removeListener, getLogConfig, topicIdPartition, maybeSendPartitionToTransactionCoordinator |
| BrokerMetadataPublisher    | applyDelta, startup, startHighWatermarkCheckPointThread                                                                                                                        |
| TransactionStateManager    | getLog, getLogEndOffset, topicIdPartition, appendRecords                                                                                                                       |
| DelayedFetch               | readFromLog, getPartitionOrException, shouldLeaderThrottle                                                                                                                     |
| LocalLeaderEndPoint        | fetchMessages, getPartitionOrException, localLogOrException, futureLocalLogOrException                                                                                         |
| RemoteLeaderEndPoint       | getPartitionOrException                                                                                                                                                        |

这个分析展示了 ReplicaManager 作为 Kafka 复制系统的核心组件，提供了完整的分区管理、数据读写、副本同步等功能，被系统的多个关键组件调用。