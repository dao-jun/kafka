/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.cluster.Partition
import kafka.log.LogManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.ReplicaManager.isListOffsetsTimestampUnsupported
import kafka.server.share.DelayedShareFetch
import org.apache.kafka.common.errors.{CorruptRecordException, FencedLeaderEpochException, InconsistentTopicIdException, InvalidProducerEpochException, InvalidTopicException, KafkaStorageException, NotLeaderOrFollowerException, OffsetNotAvailableException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, ReplicaNotAvailableException, UnknownLeaderEpochException, UnknownTopicIdException, UnknownTopicOrPartitionException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.{DeleteRecordsResponseData, DescribeLogDirsResponseData, ListOffsetsRequestData, ListOffsetsResponseData}
import org.apache.kafka.common.{IsolationLevel, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, RecordValidationStats}
import org.apache.kafka.common.requests.{FetchRequest, ListOffsetsRequest, ListOffsetsResponse, ProduceResponse}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.ActionQueue
import org.apache.kafka.server.common.{DirectoryEventHandler, RequestLocal}
import org.apache.kafka.server.log.remote.storage.RemoteLogManager
import org.apache.kafka.server.purgatory.{DelayedDeleteRecords, DelayedOperationPurgatory, DelayedRemoteFetch, DelayedRemoteListOffsets, ListOffsetsPartitionStatus, TopicPartitionOperationKey}
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchDataInfo, LogAppendInfo, LogDirFailureChannel, LogOffsetMetadata, LogReadInfo, LogReadResult, OffsetResultHolder, RecordValidationException, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.util
import java.util.{Optional, OptionalInt, OptionalLong}
import java.util.concurrent.{CompletableFuture, CompletionException, ConcurrentLinkedDeque}
import java.util.function.Consumer
import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MutableMapHasAsJava, SeqHasAsJava}
import scala.jdk.OptionConverters.RichOptional

class AsyncReplicaManager(override val config: KafkaConfig,
                          metrics: Metrics,
                          time: Time,
                          scheduler: Scheduler,
                          override val logManager: LogManager,
                          override val remoteLogManager: Option[RemoteLogManager],
                          quotaManagers: QuotaManagers,
                          override val metadataCache: MetadataCache,
                          logDirFailureChannel: LogDirFailureChannel,
                          override val alterPartitionManager: AlterPartitionManager,
                          override val brokerTopicStats: BrokerTopicStats,
                          delayedProducePurgatoryParam: Option[DelayedOperationPurgatory[DelayedProduce]],
                          delayedFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedFetch]],
                          delayedDeleteRecordsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedDeleteRecords]],
                          delayedRemoteFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteFetch]],
                          delayedRemoteListOffsetsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteListOffsets]],
                          delayedShareFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedShareFetch]],
                          override val brokerEpochSupplier: () => Long,
                          addPartitionsToTxnManager: Option[AddPartitionsToTxnManager],
                          override val directoryEventHandler: DirectoryEventHandler,
                          override val defaultActionQueue: ActionQueue
                         )
  extends ReplicaManager(config,
    metrics,
    time,
    scheduler,
    logManager,
    remoteLogManager,
    quotaManagers,
    metadataCache,
    logDirFailureChannel,
    alterPartitionManager,
    brokerTopicStats,
    delayedProducePurgatoryParam,
    delayedFetchPurgatoryParam,
    delayedDeleteRecordsPurgatoryParam,
    delayedRemoteFetchPurgatoryParam,
    delayedRemoteListOffsetsPurgatoryParam,
    delayedShareFetchPurgatoryParam,
    brokerEpochSupplier,
    addPartitionsToTxnManager,
    directoryEventHandler,
    defaultActionQueue) {

  override def asyncLogModeEnable: Boolean = config.asyncLogModeEnable

  override def startup(): Unit = {
    addPartitionsToTxnManager.foreach(_.start())
    remoteLogManager.foreach(rlm => rlm.setDelayedOperationPurgatory(delayedRemoteListOffsetsPurgatory))
  }

  override def fetchMessages(
    params: FetchParams,
    fetchInfos: collection.Seq[(TopicIdPartition, FetchRequest.PartitionData)],
    quota: ReplicaQuota,
    responseCallback: collection.Seq[(TopicIdPartition, FetchPartitionData)] => Unit
    ): Unit = {

    val logReadResultsFutures = readFromLogAsync(params, fetchInfos, quota, readFromPurgatory = false)
    CompletableFuture.allOf(logReadResultsFutures.map(f => f._2).toArray: _*)
      .thenAccept(_ => {
        val logReadResults = logReadResultsFutures.map { case (topicIdPartition, future) =>
          topicIdPartition -> future.join()
        }
        responseCallback(logReadResults.map { case (topicIdPartition, logReadResult) => topicIdPartition -> logReadResult.toFetchPartitionData(params.isFromFollower) })
      })
  }

  override def appendRecordsToLeaderAsync(
    requiredAcks: Short,
    internalTopicsAllowed: Boolean,
    origin: AppendOrigin,
    entriesPerPartition: collection.Map[TopicIdPartition, MemoryRecords],
    requestLocal: RequestLocal,
    actionQueue: ActionQueue,
    verificationGuards: collection.Map[TopicPartition, VerificationGuard],
    transactionVersion: Short
    ): collection.Map[TopicIdPartition, CompletableFuture[LogAppendResult]] = {
    val startTimeMs = time.milliseconds
    val localProduceResultsWithTopicId = appendToLocalLogAsync(
      internalTopicsAllowed = internalTopicsAllowed,
      origin,
      entriesPerPartition,
      requiredAcks,
      requestLocal,
      verificationGuards.toMap,
      transactionVersion
    )
    debug("Produce to local log in %d ms".format(time.milliseconds - startTimeMs))

    CompletableFuture.allOf(localProduceResultsWithTopicId.values.toArray: _*)
      .thenAccept(_ => addCompletePurgatoryAction(actionQueue, localProduceResultsWithTopicId.map(kv => kv._1 -> kv._2.join())))

    localProduceResultsWithTopicId
  }

  override def appendRecords(
    timeout: Long,
    requiredAcks: Short,
    internalTopicsAllowed: Boolean,
    origin: AppendOrigin,
    entriesPerPartition: collection.Map[TopicIdPartition, MemoryRecords],
    responseCallback: collection.Map[TopicIdPartition, ProduceResponse.PartitionResponse] => Unit,
    recordValidationStatsCallback: collection.Map[TopicIdPartition, RecordValidationStats] => Unit,
    requestLocal: RequestLocal,
    verificationGuards: collection.Map[TopicPartition, VerificationGuard],
    transactionVersion: Short
    ): Unit = {
    val localProduceResultFutures = appendRecordsToLeaderAsync(
      requiredAcks,
      internalTopicsAllowed,
      origin,
      entriesPerPartition,
      requestLocal,
      defaultActionQueue,
      verificationGuards,
      transactionVersion
    )
    CompletableFuture.allOf(localProduceResultFutures.values.toArray: _*)
      .thenAccept(_ => {
        val localProduceResults = localProduceResultFutures.map(kv => kv._1 -> kv._2.join())
        val produceStatus = buildProducePartitionStatus(localProduceResults)
        recordValidationStatsCallback(localProduceResults.map { case (k, v) =>
          k -> v.info.recordValidationStats
        })
        maybeAddDelayedProduce(requiredAcks, timeout, entriesPerPartition, localProduceResults, produceStatus, responseCallback)
      })
  }

  override def fetchOffset(
    topics: collection.Seq[ListOffsetsRequestData.ListOffsetsTopic],
    duplicatePartitions: collection.Set[TopicPartition],
    isolationLevel: IsolationLevel,
    replicaId: Int,
    clientId: String,
    correlationId: Int,
    version: Short,
    buildErrorResponse: (Errors, ListOffsetsRequestData.ListOffsetsPartition) => ListOffsetsResponseData.ListOffsetsPartitionResponse,
    responseCallback: Consumer[util.Collection[ListOffsetsResponseData.ListOffsetsTopicResponse]],
    timeoutMs: Int
    ): Unit = {

    val statusByPartition = mutable.Map[TopicPartition, ListOffsetsPartitionStatus]()
    val futures = mutable.Map[TopicPartition, CompletableFuture[OffsetResultHolder]]()
    val partitionsToProcess = mutable.ListBuffer[TopicPartition]()

    // 辅助方法：处理错误并构建状态
    def handlePartitionError(e: Throwable, topicPartition: TopicPartition, partition: ListOffsetsPartition): Unit = {
      e match {
        case _@(_: UnknownTopicOrPartitionException |
                _: NotLeaderOrFollowerException |
                _: UnknownLeaderEpochException |
                _: FencedLeaderEpochException |
                _: KafkaStorageException |
                _: UnsupportedForMessageFormatException) =>
          debug(s"Offset request with correlation id $correlationId from client $clientId on " +
            s"partition $topicPartition failed due to ${e.getMessage}")
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()

        case e: OffsetNotAvailableException =>
          val error = if (version >= 5) Errors.forException(e) else Errors.LEADER_NOT_AVAILABLE
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(error, partition))).build()

        case e: Throwable =>
          error("Error while responding to offset request", e)
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
      }
    }

    // 辅助方法：处理成功的结果
    def handlePartitionSuccess(resultHolder: OffsetResultHolder, topicPartition: TopicPartition, partition: ListOffsetsPartition): Unit = {
      val status = {
        if (resultHolder.timestampAndOffsetOpt().isPresent) {
          // This case is for normal topic that does not have remote storage.
          val timestampAndOffsetOpt = resultHolder.timestampAndOffsetOpt.get
          var partitionResponse = buildErrorResponse(Errors.NONE, partition)
          if (resultHolder.lastFetchableOffset.isPresent &&
            timestampAndOffsetOpt.offset >= resultHolder.lastFetchableOffset.get) {
            resultHolder.maybeOffsetsError.map(e => throw e)
          } else {
            partitionResponse = new ListOffsetsPartitionResponse()
              .setPartitionIndex(partition.partitionIndex)
              .setErrorCode(Errors.NONE.code)
              .setTimestamp(timestampAndOffsetOpt.timestamp)
              .setOffset(timestampAndOffsetOpt.offset)
            if (timestampAndOffsetOpt.leaderEpoch.isPresent && version >= 4)
              partitionResponse.setLeaderEpoch(timestampAndOffsetOpt.leaderEpoch.get)
          }
          ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(partitionResponse)).build()
        } else if (resultHolder.timestampAndOffsetOpt.isEmpty && resultHolder.futureHolderOpt.isEmpty) {
          // This is an empty offset response scenario
          resultHolder.maybeOffsetsError.map(e => throw e)
          ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.NONE, partition))).build()
        } else if (resultHolder.timestampAndOffsetOpt.isEmpty && resultHolder.futureHolderOpt.isPresent) {
          // This case is for topic enabled with remote storage and we want to search the timestamp in
          // remote storage using async fashion.
          ListOffsetsPartitionStatus.builder()
            .futureHolderOpt(resultHolder.futureHolderOpt())
            .lastFetchableOffset(resultHolder.lastFetchableOffset)
            .maybeOffsetsError(resultHolder.maybeOffsetsError)
            .build()
        } else {
          throw new IllegalStateException(s"Unexpected result holder state $resultHolder")
        }
      }
      statusByPartition += topicPartition -> status
    }

    // 辅助方法：发送响应
    def sendResponse(): Unit = {
      if (delayedRemoteListOffsetsRequired(statusByPartition)) {
        val delayMs: Long = if (timeoutMs > 0) timeoutMs else config.remoteLogManagerConfig.remoteListOffsetsRequestTimeoutMs()
        val delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, version, statusByPartition.asJava,
          tp => getPartitionOrException(tp), responseCallback)
        val listOffsetsRequestKeys = statusByPartition.keys.map(new TopicPartitionOperationKey(_)).toList
        delayedRemoteListOffsetsPurgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys.asJava)
      } else {
        val responseTopics = statusByPartition.groupBy(e => e._1.topic()).map {
          case (topic, status) =>
            new ListOffsetsTopicResponse()
              .setName(topic)
              .setPartitions(status.values.flatMap(s => Some(s.responseOpt.get())).toList.asJava)
        }.toList
        responseCallback.accept(responseTopics.asJava)
      }
    }

    // 第一步：收集所有需要处理的partition
    topics.foreach { topic =>
      topic.partitions.asScala.foreach { partition =>
        val topicPartition = new TopicPartition(topic.name, partition.partitionIndex)

        if (duplicatePartitions.contains(topicPartition)) {
          debug(s"OffsetRequest with correlation id $correlationId from client $clientId on partition $topicPartition " +
            s"failed because the partition is duplicated in the request.")
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.INVALID_REQUEST, partition))).build()
        } else if (isListOffsetsTimestampUnsupported(partition.timestamp(), version)) {
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.UNSUPPORTED_VERSION, partition))).build()
        } else {
          try {
            val fetchOnlyFromLeader = replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID
            val isClientRequest = replicaId == ListOffsetsRequest.CONSUMER_REPLICA_ID
            val isolationLevelOpt = if (isClientRequest) Some(isolationLevel) else None

            val future = fetchOffsetForTimestampAsync(
              topicPartition,
              partition.timestamp,
              isolationLevelOpt,
              if (partition.currentLeaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) Optional.empty()
              else Optional.of(partition.currentLeaderEpoch),
              fetchOnlyFromLeader
            )

            futures += topicPartition -> future
            partitionsToProcess += topicPartition

          } catch {
            case e: Throwable =>
              handlePartitionError(e, topicPartition, partition)
          }
        }
      }
    }

    // 第二步：等待所有异步调用完成
    if (futures.nonEmpty) {
      val futureArray = futures.values.toArray

      CompletableFuture.allOf(futureArray: _*).whenComplete { (_, exception) =>
        if (exception != null) {
          error("Error while processing offset requests", exception)
        }

        // 处理每个future的结果
        partitionsToProcess.foreach { topicPartition =>
          val future = futures(topicPartition)
          val topic = topics.find(_.name == topicPartition.topic()).get
          val partition = topic.partitions.asScala.find(_.partitionIndex == topicPartition.partition()).get

          try {
            if (future.isCompletedExceptionally) {
              // 处理异常情况
              future.exceptionally { ex =>
                ex match {
                  case e: CompletionException => handlePartitionError(e.getCause, topicPartition, partition)
                  case e: Throwable => handlePartitionError(e, topicPartition, partition)
                }
                null
              }.join()
            } else {
              try {
                val resultHolder = future.get()
                handlePartitionSuccess(resultHolder, topicPartition, partition)
              } catch {
                case e: Throwable =>
                  handlePartitionError(e, topicPartition, partition)
              }
            }
          } catch {
            case e: Throwable =>
              error("Unexpected error while processing offset result", e)
              statusByPartition += topicPartition ->
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
          }
        }

        sendResponse()
      }
    } else {
      sendResponse()
    }
  }

  override def fetchOffsetForTimestampAsync(
    topicPartition: TopicPartition,
    timestamp: Long,
    isolationLevel: Option[IsolationLevel],
    currentLeaderEpoch: Optional[Integer],
    fetchOnlyFromLeader: Boolean
    ): CompletableFuture[OffsetResultHolder] = {
    val partition = getPartitionOrException(topicPartition)
    partition.fetchOffsetForTimestampAsync(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader, remoteLogManager)
  }

  override def readFromLogAsync(
    params: FetchParams,
    readPartitionInfo: collection.Seq[(TopicIdPartition, FetchRequest.PartitionData)],
    quota: ReplicaQuota,
    readFromPurgatory: Boolean
    ): collection.Seq[(TopicIdPartition, CompletableFuture[LogReadResult])] = {
    val traceEnabled = isTraceEnabled

    def checkFetchDataInfo(partition: Partition, givenFetchedDataInfo: FetchDataInfo) = {
      if (params.isFromFollower && shouldLeaderThrottle(quota, partition, params.replicaId)) {
        // If the partition is being throttled, simply return an empty set.
        new FetchDataInfo(givenFetchedDataInfo.fetchOffsetMetadata, MemoryRecords.EMPTY)
      } else if (givenFetchedDataInfo.firstEntryIncomplete) {
        // Replace incomplete message sets with an empty one as consumers can make progress in such
        // cases and don't need to report a `RecordTooLargeException`
        new FetchDataInfo(givenFetchedDataInfo.fetchOffsetMetadata, MemoryRecords.EMPTY)
      } else {
        givenFetchedDataInfo
      }
    }

    def read(tp: TopicIdPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): CompletableFuture[LogReadResult] = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      var log: UnifiedLog = null
      var partition: Partition = null
      val fetchTimeMs = time.milliseconds
      try {
        if (traceEnabled)
          trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
            s"remaining response limit $limitBytes" +
            (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        partition = getPartitionOrException(tp.topicPartition)

        // Check if topic ID from the fetch request/session matches the ID in the log
        val topicId = if (tp.topicId == Uuid.ZERO_UUID) None else Some(tp.topicId)
        if (!hasConsistentTopicId(topicId, partition.topicId))
          throw new InconsistentTopicIdException("Topic ID in the fetch session did not match the topic ID in the log.")

        // If we are the leader, determine the preferred read-replica
        val preferredReadReplica = params.clientMetadata.toScala.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, params.replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorPlugin.foreach { selector =>
            debug(s"Replica selector ${selector.get.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for ${params.clientMetadata}")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          CompletableFuture.completedFuture(
            new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
              Optional.empty(),
              offsetSnapshot.highWatermark.messageOffset,
              offsetSnapshot.logStartOffset,
              offsetSnapshot.logEndOffset.messageOffset,
              followerLogStartOffset,
              -1L,
              OptionalLong.of(offsetSnapshot.lastStableOffset.messageOffset),
              if (preferredReadReplica.isDefined) OptionalInt.of(preferredReadReplica.get) else OptionalInt.empty(),
              Errors.NONE))
        } else {
          log = partition.localLogWithEpochOrThrow(fetchInfo.currentLeaderEpoch, params.fetchOnlyLeader())

          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          val readInfoFuture: CompletableFuture[LogReadInfo] = partition.fetchRecordsAsync(
            fetchParams = params,
            fetchPartitionData = fetchInfo,
            fetchTimeMs = fetchTimeMs,
            maxBytes = adjustedMaxBytes,
            minOneMessage = minOneMessage,
            updateFetchState = !readFromPurgatory)
          readInfoFuture.thenApply(readInfo => {
              val fetchDataInfo = checkFetchDataInfo(partition, readInfo.fetchedData)

              new LogReadResult(fetchDataInfo,
                readInfo.divergingEpoch,
                readInfo.highWatermark,
                readInfo.logStartOffset,
                readInfo.logEndOffset,
                followerLogStartOffset,
                fetchTimeMs,
                OptionalLong.of(readInfo.lastStableOffset),
                if (preferredReadReplica.isDefined) OptionalInt.of(preferredReadReplica.get) else OptionalInt.empty(),
                Errors.NONE
              )
            })
            .exceptionally(t => {
              handleFetchMessageError(t, tp, params, fetchInfo, adjustedMaxBytes, minOneMessage, log, fetchTimeMs)
            })
        }
      } catch {
        case t: Throwable => CompletableFuture.completedFuture(handleFetchMessageError(t, tp, params, fetchInfo, adjustedMaxBytes, minOneMessage, log, fetchTimeMs))
      }
    }

    var limitBytes = params.maxBytes
    val result = new ConcurrentLinkedDeque[(TopicIdPartition, CompletableFuture[LogReadResult])]()
    var minOneMessage = true
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResultFuture = read(tp, fetchInfo, limitBytes, minOneMessage)
      readResultFuture.thenAccept { readResult =>
        val recordBatchSize = readResult.info.records.sizeInBytes
        // Because we don't know how much data will be retrieved in remote fetch yet, and we don't want to block the API call
        // to query remoteLogMetadata, assume it will fetch the max bytes size of data to avoid to exceed the "fetch.max.bytes" setting.
        val estimatedRecordBatchSize = if (recordBatchSize == 0 && readResult.info.delayedRemoteStorageFetch.isPresent)
          readResult.info.delayedRemoteStorageFetch.get.fetchMaxBytes else recordBatchSize
        // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
        if (estimatedRecordBatchSize > 0)
          minOneMessage = false
        limitBytes = math.max(0, limitBytes - estimatedRecordBatchSize)
      }
      result.add(tp -> readResultFuture)
    }
    result.asScala.toSeq
  }

  override def readFromLog(
    params: FetchParams,
    readPartitionInfo: collection.Seq[(TopicIdPartition, PartitionData)],
    quota: ReplicaQuota, readFromPurgatory: Boolean
    ): collection.Seq[(TopicIdPartition, LogReadResult)] = {
    error("Read from log sync is unsupported in AsyncReplicaManager")
    readFromLogAsync(params, readPartitionInfo, quota, readFromPurgatory)
      .map {
        case (tp, future) =>
          tp -> future.join()
      }
  }

  override def fetchOffsetForTimestamp(
    topicPartition: TopicPartition,
    timestamp: Long,
    isolationLevel: Option[IsolationLevel],
    currentLeaderEpoch: Optional[Integer],
    fetchOnlyFromLeader: Boolean
    ): OffsetResultHolder = {
    error("Fetch offset for timestamp sync is unsupported in AsyncReplicaManager")
    fetchOffsetForTimestampAsync(topicPartition, timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader).join()
  }

  private def handleFetchMessageError(
    t: Throwable,
    tp: TopicIdPartition,
    params: FetchParams,
    fetchInfo: PartitionData,
    adjustedMaxBytes: Int,
    minOneMessage: Boolean,
    log: UnifiedLog,
    fetchTimeMs: Long
    ): LogReadResult = {
    t match {
      // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
      // is supposed to indicate un-expected failure of a broker in handling a fetch request
      case e@(_: UnknownTopicOrPartitionException |
              _: NotLeaderOrFollowerException |
              _: UnknownLeaderEpochException |
              _: FencedLeaderEpochException |
              _: ReplicaNotAvailableException |
              _: KafkaStorageException |
              _: InconsistentTopicIdException) =>
        new LogReadResult(Errors.forException(e))
      case e: OffsetOutOfRangeException =>
        handleOffsetOutOfRangeError(tp, params, fetchInfo, adjustedMaxBytes, minOneMessage, log, fetchTimeMs, e)
      case e: Throwable =>
        brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
        brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

        val fetchSource = FetchRequest.describeReplicaId(params.replicaId)
        error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
          s"on partition $tp: $fetchInfo", e)
        new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
          Optional.empty(),
          UnifiedLog.UNKNOWN_OFFSET,
          UnifiedLog.UNKNOWN_OFFSET,
          UnifiedLog.UNKNOWN_OFFSET,
          UnifiedLog.UNKNOWN_OFFSET,
          -1L,
          OptionalLong.empty(),
          Errors.forException(e)
        )
    }
  }

  override def deleteRecords(
    timeout: Long,
    offsetPerPartition: collection.Map[TopicPartition, Long],
    responseCallback: collection.Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] => Unit,
    allowInternalTopicDeletion: Boolean
    ): Unit = {
    responseCallback(offsetPerPartition.keySet.map(tp => tp -> new DeleteRecordsResponseData.DeleteRecordsPartitionResult()).toMap)
  }

  private def appendToLocalLogAsync(
    internalTopicsAllowed: Boolean,
    origin: AppendOrigin,
    entriesPerPartition: collection.Map[TopicIdPartition, MemoryRecords],
    requiredAcks: Short,
    requestLocal: RequestLocal,
    verificationGuards: collection.Map[TopicPartition, VerificationGuard],
    transactionVersion: Short
    ): collection.Map[TopicIdPartition, CompletableFuture[LogAppendResult]] = {
    val traceEnabled = isTraceEnabled

    def processFailedRecord(topicIdPartition: TopicIdPartition, t: Throwable) = {
      val logStartOffset = onlinePartition(topicIdPartition.topicPartition()).map(_.logStartOffset).getOrElse(-1L)
      brokerTopicStats.topicStats(topicIdPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      t match {
        case _: InvalidProducerEpochException =>
          info(s"Error processing append operation on partition $topicIdPartition", t)
        case _ =>
          error(s"Error processing append operation on partition $topicIdPartition", t)
      }

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicIdPartition, records) =>
      brokerTopicStats.topicStats(topicIdPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicIdPartition.topic) && !internalTopicsAllowed) {
        (topicIdPartition, CompletableFuture.completedFuture(LogAppendResult(
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicIdPartition.topic}")),
          hasCustomErrorMessage = false)))
      } else {
        try {
          val partition = getPartitionOrException(topicIdPartition)
          val infoFuture = partition.appendRecordsToLeaderAsync(records, origin, requiredAcks, requestLocal,
            verificationGuards.getOrElse(topicIdPartition.topicPartition(), VerificationGuard.SENTINEL), transactionVersion)

          val resultFuture: CompletableFuture[LogAppendResult] = infoFuture
            .thenApply(info => {
              val numAppendedMessages = info.numMessages
              brokerTopicStats.topicStats(topicIdPartition.topic).bytesInRate.mark(records.sizeInBytes)
              brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
              brokerTopicStats.topicStats(topicIdPartition.topic).messagesInRate.mark(numAppendedMessages)
              brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)
              if (traceEnabled)
                trace(s"${records.sizeInBytes} written to log $topicIdPartition beginning at offset " +
                  s"${info.firstOffset} and ending at offset ${info.lastOffset}")
              LogAppendResult(info, exception = None, hasCustomErrorMessage = false)
            })
            .exceptionally(t => {
              handleAppendError(topicIdPartition, t, processFailedRecord)
            })
          (topicIdPartition, resultFuture)
        } catch {
          case e: Throwable =>
            (topicIdPartition, CompletableFuture.completedFuture(handleAppendError(topicIdPartition, e, processFailedRecord)))
        }
      }
    }
  }

  private def handleAppendError(
    topicIdPartition: TopicIdPartition,
    t: Throwable,
    processFailedRecord: (TopicIdPartition, Throwable) => Long
    ): LogAppendResult = {
    t match {
      case e@(_: UnknownTopicOrPartitionException |
              _: NotLeaderOrFollowerException |
              _: RecordTooLargeException |
              _: RecordBatchTooLargeException |
              _: CorruptRecordException |
              _: KafkaStorageException |
              _: UnknownTopicIdException) =>
        LogAppendResult(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO, Some(e), hasCustomErrorMessage = false)
      case rve: RecordValidationException =>
        val logStartOffset = processFailedRecord(topicIdPartition, rve.invalidException)
        val recordErrors = rve.recordErrors
        LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(logStartOffset, recordErrors),
          Some(rve.invalidException), hasCustomErrorMessage = true)
      case t: Throwable =>
        val logStartOffset = processFailedRecord(topicIdPartition, t)
        LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset),
          Some(t), hasCustomErrorMessage = false)
    }
  }

  override def alterReplicaLogDirs(
    partitionDirs: collection.Map[TopicPartition, String]
    ): collection.Map[TopicPartition, Errors] = {
    val responseMap = mutable.HashMap[TopicPartition, Errors]()
    partitionDirs.keySet.foreach(tp => {
      onlinePartition(tp) match {
        case Some(_) =>
          responseMap += tp -> Errors.NONE
        case None =>
          if (metadataCache.contains(tp)) {
            responseMap += tp -> Errors.NOT_LEADER_OR_FOLLOWER
          } else {
            responseMap += tp -> Errors.UNKNOWN_TOPIC_OR_PARTITION
          }
      }
    })
    responseMap
  }

  override def describeLogDirs(
    partitions: collection.Set[TopicPartition]
  ): util.List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val response = new util.ArrayList[DescribeLogDirsResponseData.DescribeLogDirsResult]()
    response
  }
}
