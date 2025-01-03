package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.END_OF_PUSH;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;

import com.linkedin.avroutil1.compatibility.shaded.org.apache.commons.lang3.Validate;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.davinci.validation.PartitionTracker.TopicType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.ValueHolder;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorePartitionDataReceiver
    implements ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> {
  private final StoreIngestionTask storeIngestionTask;
  private final PubSubTopicPartition topicPartition;
  private final String kafkaUrl;
  private final String kafkaUrlForLogger;
  private final int kafkaClusterId;
  private final Logger LOGGER;

  private long receivedRecordsCount;

  public StorePartitionDataReceiver(
      StoreIngestionTask storeIngestionTask,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId) {
    this.storeIngestionTask = Validate.notNull(storeIngestionTask);
    this.topicPartition = Validate.notNull(topicPartition);
    this.kafkaUrl = Validate.notNull(kafkaUrl);
    this.kafkaUrlForLogger = Utils.getSanitizedStringForLogger(kafkaUrl);
    this.kafkaClusterId = kafkaClusterId;
    this.LOGGER = LogManager.getLogger(this.getClass().getSimpleName() + " [" + kafkaUrlForLogger + "]");
    this.receivedRecordsCount = 0L;
  }

  @Override
  public void write(List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumedData) throws Exception {
    receivedRecordsCount += consumedData.size();
    try {
      /**
       * This function could be blocked by the following reasons:
       * 1. The pre-condition is not satisfied before producing to the shared StoreBufferService, such as value schema is not available;
       * 2. The producing is blocked by the throttling of the shared StoreBufferService;
       *
       * For #1, it is acceptable since there is a timeout for the blocking logic, and it doesn't happen very often
       * based on the operational experience;
       * For #2, the blocking caused by throttling is expected since all the ingestion tasks are sharing the
       * same StoreBufferService;
       *
       * If there are changes with the above assumptions or new blocking behaviors, we need to evaluate whether
       * we need to do some kind of isolation here, otherwise the consumptions for other store versions with the
       * same shared consumer will be affected.
       * The potential isolation strategy is:
       * 1. When detecting such kind of prolonged or infinite blocking, the following function should expose a
       * param to decide whether it should return early in those conditions;
       * 2. Once this function realizes this behavior, it could choose to temporarily {@link PubSubConsumerAdapter#pause}
       * the blocked consumptions;
       * 3. This runnable could {@link PubSubConsumerAdapter#resume} the subscriptions after some delays or
       * condition change, and there are at least two ways to make the subscription resumption without missing messages:
       * a. Keep the previous message leftover in this class and retry, and once the messages can be processed
       * without blocking, then resume the paused subscriptions;
       * b. Don't keep the message leftover in this class, but every time, rewind the offset to the checkpointed offset
       * of the corresponding {@link StoreIngestionTask} and resume subscriptions;
       *
       * For option #a, the logic is simpler and but the concern is that
       * the buffered messages inside the shared consumer and the message leftover could potentially cause
       * some GC issue, and option #b won't have this problem since {@link PubSubConsumerAdapter#pause} will drop
       * all the buffered messages for the paused partitions, but just slightly more complicate.
       *
       */
      produceToStoreBufferServiceOrKafka(consumedData, topicPartition, kafkaUrl, kafkaClusterId);
    } catch (Exception e) {
      handleDataReceiverException(e);
    }
  }

  /**
   * This function is in charge of producing the consumer records to the writer buffers maintained by {@link StoreBufferService}.
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
   *
   * @param records : received consumer records
   */
  protected void produceToStoreBufferServiceOrKafka(
      Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId) throws InterruptedException {
    final int partition = topicPartition.getPartitionNumber();
    PartitionConsumptionState partitionConsumptionState = storeIngestionTask.getPartitionConsumptionState(partition);
    if (partitionConsumptionState == null) {
      throw new VeniceException(
          "PartitionConsumptionState should present for store version: " + storeIngestionTask.getKafkaVersionTopic()
              + ", partition: " + partition);
    }

    /**
     * Validate and filter out duplicate messages from the real-time topic as early as possible, so that
     * the following batch processing logic won't spend useless efforts on duplicate messages.
     */
    records = validateAndFilterOutDuplicateMessagesFromLeaderTopic(records, kafkaUrl, topicPartition);

    if (storeIngestionTask.shouldProduceInBatch(records)) {
      produceToStoreBufferServiceOrKafkaInBatch(
          records,
          topicPartition,
          partitionConsumptionState,
          kafkaUrl,
          kafkaClusterId);
      return;
    }

    long totalBytesRead = 0;
    ValueHolder<Double> elapsedTimeForPuttingIntoQueue = new ValueHolder<>(0d);
    boolean metricsEnabled = storeIngestionTask.isMetricsEmissionEnabled();
    long beforeProcessingBatchRecordsTimestampMs = System.currentTimeMillis();

    for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record: records) {
      long beforeProcessingPerRecordTimestampNs = System.nanoTime();
      partitionConsumptionState.setLatestPolledMessageTimestampInMs(beforeProcessingBatchRecordsTimestampMs);
      if (!storeIngestionTask.shouldProcessRecord(record)) {
        partitionConsumptionState.updateLatestIgnoredUpstreamRTOffset(kafkaUrl, record.getOffset());
        continue;
      }

      // Check schema id availability before putting consumer record to drainer queue
      waitReadyToProcessRecord(record);

      totalBytesRead += handleSingleMessage(
          new PubSubMessageProcessedResultWrapper<>(record),
          topicPartition,
          partitionConsumptionState,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingPerRecordTimestampNs,
          beforeProcessingBatchRecordsTimestampMs,
          metricsEnabled,
          elapsedTimeForPuttingIntoQueue);
    }

    updateMetricsAndEnforceQuota(
        totalBytesRead,
        partition,
        elapsedTimeForPuttingIntoQueue,
        beforeProcessingBatchRecordsTimestampMs);
  }

  public void produceToStoreBufferServiceOrKafkaInBatch(
      Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records,
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId) throws InterruptedException {
    long totalBytesRead = 0;
    ValueHolder<Double> elapsedTimeForPuttingIntoQueue = new ValueHolder<>(0d);
    boolean metricsEnabled = storeIngestionTask.isMetricsEmissionEnabled();
    long beforeProcessingBatchRecordsTimestampMs = System.currentTimeMillis();
    /**
     * Split the records into mini batches.
     */
    int batchSize = storeIngestionTask.getServerConfig().getAAWCWorkloadParallelProcessingThreadPoolSize();
    List<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> batches = new ArrayList<>();
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> ongoingBatch = new ArrayList<>(batchSize);
    Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> iter = records.iterator();
    while (iter.hasNext()) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = iter.next();
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLatestPolledMessageTimestampInMs(beforeProcessingBatchRecordsTimestampMs);
      }
      if (!storeIngestionTask.shouldProcessRecord(record)) {
        if (partitionConsumptionState != null) {
          partitionConsumptionState.updateLatestIgnoredUpstreamRTOffset(kafkaUrl, record.getOffset());
        }
        continue;
      }
      waitReadyToProcessRecord(record);
      ongoingBatch.add(record);
      if (ongoingBatch.size() == batchSize) {
        batches.add(ongoingBatch);
        ongoingBatch = new ArrayList<>(batchSize);
      }
    }
    if (!ongoingBatch.isEmpty()) {
      batches.add(ongoingBatch);
    }
    if (batches.isEmpty()) {
      return;
    }
    IngestionBatchProcessor ingestionBatchProcessor = storeIngestionTask.getIngestionBatchProcessor();
    if (ingestionBatchProcessor == null) {
      throw new VeniceException(
          "IngestionBatchProcessor object should present for store version: "
              + storeIngestionTask.getKafkaVersionTopic());
    }
    /**
     * Process records batch by batch.
     */
    for (List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> batch: batches) {
      NavigableMap<ByteArrayKey, ReentrantLock> keyLockMap = ingestionBatchProcessor.lockKeys(batch);
      try {
        long beforeProcessingPerRecordTimestampNs = System.nanoTime();
        List<PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long>> processedResults =
            ingestionBatchProcessor.process(
                batch,
                partitionConsumptionState,
                topicPartition.getPartitionNumber(),
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingPerRecordTimestampNs,
                beforeProcessingBatchRecordsTimestampMs);

        for (PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> processedRecord: processedResults) {
          totalBytesRead += handleSingleMessage(
              processedRecord,
              topicPartition,
              partitionConsumptionState,
              kafkaUrl,
              kafkaClusterId,
              beforeProcessingPerRecordTimestampNs,
              beforeProcessingBatchRecordsTimestampMs,
              metricsEnabled,
              elapsedTimeForPuttingIntoQueue);
        }
      } finally {
        ingestionBatchProcessor.unlockKeys(keyLockMap);
      }
    }

    updateMetricsAndEnforceQuota(
        totalBytesRead,
        topicPartition.getPartitionNumber(),
        elapsedTimeForPuttingIntoQueue,
        beforeProcessingBatchRecordsTimestampMs);
  }

  private void updateMetricsAndEnforceQuota(
      long totalBytesRead,
      int partition,
      ValueHolder<Double> elapsedTimeForPuttingIntoQueue,
      long beforeProcessingBatchRecordsTimestampMs) {
    /**
     * Even if the records list is empty, we still need to check quota to potentially resume partition
     */
    final StorageUtilizationManager storageUtilizationManager = storeIngestionTask.getStorageUtilizationManager();
    storageUtilizationManager.enforcePartitionQuota(partition, totalBytesRead);

    if (storeIngestionTask.isMetricsEmissionEnabled()) {
      HostLevelIngestionStats hostLevelIngestionStats = storeIngestionTask.getHostLevelIngestionStats();
      if (totalBytesRead > 0) {
        hostLevelIngestionStats.recordTotalBytesReadFromKafkaAsUncompressedSize(totalBytesRead);
      }
      if (elapsedTimeForPuttingIntoQueue.getValue() > 0) {
        hostLevelIngestionStats.recordConsumerRecordsQueuePutLatency(
            elapsedTimeForPuttingIntoQueue.getValue(),
            beforeProcessingBatchRecordsTimestampMs);
      }

      hostLevelIngestionStats.recordStorageQuotaUsed(storageUtilizationManager.getDiskQuotaUsage());
    }
  }

  public Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> validateAndFilterOutDuplicateMessagesFromLeaderTopic(
      Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records,
      String kafkaUrl,
      PubSubTopicPartition topicPartition) {
    PartitionConsumptionState partitionConsumptionState =
        storeIngestionTask.getPartitionConsumptionState(topicPartition.getPartitionNumber());
    if (partitionConsumptionState == null) {
      // The partition is likely unsubscribed, will skip these messages.
      LOGGER.warn(
          "No partition consumption state for store version: {}, partition:{}, will filter out all the messages",
          storeIngestionTask.getKafkaVersionTopic(),
          topicPartition.getPartitionNumber());
      return Collections.emptyList();
    }
    boolean isEndOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
    if (!storeIngestionTask.shouldProduceToVersionTopic(partitionConsumptionState)) {
      return records;
    }
    /**
     * Just to note this code is getting executed in Leader only. Leader DIV check progress is always ahead of the
     * actual data persisted on disk. Leader DIV check results will not be persisted on disk.
     */
    Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> iter = records.iterator();
    while (iter.hasNext()) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = iter.next();
      boolean isRealTimeMsg = record.getTopicPartition().getPubSubTopic().isRealTime();
      try {
        /**
         * TODO: An improvement can be made to fail all future versions for fatal DIV exceptions after EOP.
         */
        TopicType topicType;
        if (storeIngestionTask.isGlobalRtDivEnabled()) {
          final int topicTypeEnumCode = isRealTimeMsg ? TopicType.REALTIME_TOPIC_TYPE : TopicType.VERSION_TOPIC_TYPE;
          topicType = TopicType.of(topicTypeEnumCode, kafkaUrl);
        } else {
          topicType = PartitionTracker.VERSION_TOPIC;
        }

        storeIngestionTask.validateMessage(
            topicType,
            storeIngestionTask.getKafkaDataIntegrityValidatorForLeaders(),
            record,
            isEndOfPushReceived,
            partitionConsumptionState);
        storeIngestionTask.getVersionedDIVStats()
            .recordSuccessMsg(storeIngestionTask.getStoreName(), storeIngestionTask.getVersionNumber());
      } catch (FatalDataValidationException e) {
        if (!isEndOfPushReceived) {
          throw e;
        }
      } catch (DuplicateDataException e) {
        /**
         * Skip duplicated messages; leader must not produce duplicated messages from RT to VT, because leader will
         * override the DIV info for messages from RT; as a result, both leaders and followers will persisted duplicated
         * messages to disk, and potentially rewind a k/v pair to an old value.
         */
        storeIngestionTask.getDivErrorMetricCallback().accept(e);
        LOGGER.debug(
            "Skipping a duplicate record from: {} offset: {} for replica: {}",
            record.getTopicPartition(),
            record.getOffset(),
            partitionConsumptionState.getReplicaId());
        iter.remove();
      }
    }
    return records;
  }

  /**
   * This method checks whether the given record needs to be checked schema availability. Only PUT and UPDATE message
   * needs to #checkValueSchemaAvail
   * @param record
   */
  private void waitReadyToProcessRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record)
      throws InterruptedException {
    KafkaMessageEnvelope kafkaValue = record.getValue();
    if (record.getKey().isControlMessage() || kafkaValue == null) {
      return;
    }

    switch (MessageType.valueOf(kafkaValue)) {
      case PUT:
        Put put = (Put) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(put.schemaId);
        try {
          storeIngestionTask.deserializeValue(put.schemaId, put.putValue, record);
        } catch (Exception e) {
          PartitionConsumptionState pcs =
              storeIngestionTask.getPartitionConsumptionState(record.getTopicPartition().getPartitionNumber());
          LeaderFollowerStateType state = pcs == null ? null : pcs.getLeaderFollowerState();
          throw new VeniceException(
              "Failed to deserialize PUT for: " + record.getTopicPartition() + ", offset: " + record.getOffset()
                  + ", schema id: " + put.schemaId + ", LF state: " + state,
              e);
        }
        break;
      case UPDATE:
        Update update = (Update) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(update.schemaId);
        break;
      case DELETE:
        /* we don't need to check schema availability for DELETE */
        break;
      default:
        throw new VeniceMessageException(
            storeIngestionTask.getIngestionTaskName() + " : Invalid/Unrecognized operation type submitted: "
                + kafkaValue.messageType);
    }
  }

  /**
   * Check whether the given schema id is available for current store.
   * The function will bypass the check if schema id is -1 (VPJ job is still using it before we finishes the integration with schema registry).
   * Right now, this function is maintaining a local cache for schema id of current store considering that the value schema is immutable;
   * If the schema id is not available, this function will polling until the schema appears or timeout: {@link StoreIngestionTask#SCHEMA_POLLING_TIMEOUT_MS};
   *
   * @param schemaId
   */
  private void waitReadyToProcessDataRecord(int schemaId) throws InterruptedException {
    if (schemaId == -1) {
      // TODO: Once Venice Client (VeniceShellClient) finish the integration with schema registry,
      // we need to remove this check here.
      return;
    }

    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()
        || schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      StoreVersionState storeVersionState = waitVersionStateAvailable(storeIngestionTask.getKafkaVersionTopic());
      if (!storeVersionState.chunked) {
        throw new VeniceException(
            "Detected chunking in a store-version where chunking is NOT enabled. Will abort ingestion.");
      }
      return;
    }

    storeIngestionTask.waitUntilValueSchemaAvailable(schemaId);
  }

  private StoreVersionState waitVersionStateAvailable(String kafkaTopic) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long elapsedTime;
    StoreVersionState state;
    for (;;) {
      state = storeIngestionTask.getStorageEngine().getStoreVersionState();
      elapsedTime = System.currentTimeMillis() - startTime;

      if (state != null) {
        return state;
      }

      if (elapsedTime > StoreIngestionTask.SCHEMA_POLLING_TIMEOUT_MS || !storeIngestionTask.isRunning()) {
        LOGGER.warn("Version state is not available for {} after {}", kafkaTopic, elapsedTime);
        throw new VeniceException("Store version state is not available for " + kafkaTopic);
      }

      Thread.sleep(StoreIngestionTask.SCHEMA_POLLING_DELAY_MS);
    }
  }

  private int handleSingleMessage(
      PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingPerRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs,
      boolean metricsEnabled,
      ValueHolder<Double> elapsedTimeForPuttingIntoQueue) throws InterruptedException {
    final int partition = topicPartition.getPartitionNumber();
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = consumerRecordWrapper.getMessage();
    if (record.getKey().isControlMessage()) {
      ControlMessage controlMessage = (ControlMessage) record.getValue().payloadUnion;
      if (ControlMessageType.valueOf(controlMessage.controlMessageType) == ControlMessageType.START_OF_PUSH) {
        /**
         * N.B.: The rest of the {@link ControlMessage} types are handled by: {@link StoreIngestionTask#processControlMessage}
         *
         * But for the SOP in particular, we want to process it here, at the start of the pipeline, to ensure that the
         * {@link StoreVersionState} is properly primed, as other functions below this point, but prior to being
         * enqueued into the {@link StoreBufferService} rely on this state to be there.
         */
        storeIngestionTask.processStartOfPush(
            record.getValue(),
            controlMessage,
            record.getTopicPartition().getPartitionNumber(),
            storeIngestionTask.getPartitionConsumptionState(partition));
      }
    }

    // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after
    // this call.
    DelegateConsumerRecordResult delegateConsumerRecordResult = delegateConsumerRecordMaybeWithLock(
        consumerRecordWrapper,
        partition,
        kafkaUrl,
        kafkaClusterId,
        beforeProcessingPerRecordTimestampNs,
        beforeProcessingBatchRecordsTimestampMs);

    switch (delegateConsumerRecordResult) {
      case QUEUED_TO_DRAINER:
        long queuePutStartTimeInNS = metricsEnabled ? System.nanoTime() : 0;

        // blocking call
        storeIngestionTask.putConsumerRecord(record, partition, kafkaUrl, beforeProcessingPerRecordTimestampNs);

        if (metricsEnabled) {
          elapsedTimeForPuttingIntoQueue.setValue(
              elapsedTimeForPuttingIntoQueue.getValue() + LatencyUtils.getElapsedTimeFromNSToMS(queuePutStartTimeInNS));
        }
        break;
      case PRODUCED_TO_KAFKA:
      case SKIPPED_MESSAGE:
        break;
      default:
        throw new VeniceException(
            storeIngestionTask.getIngestionTaskName() + " received unknown DelegateConsumerRecordResult enum for "
                + record.getTopicPartition());
    }
    // Update the latest message consumed time
    partitionConsumptionState.setLatestMessageConsumedTimestampInMs(beforeProcessingBatchRecordsTimestampMs);

    return record.getPayloadSize();
  }

  /**
   * This enum represents all potential results after calling {@link #delegateConsumerRecord(PubSubMessageProcessedResultWrapper, int, String, int, long, long)}.
   */
  protected enum DelegateConsumerRecordResult {
    /**
     * The consumer record has been produced to local version topic by leader.
     */
    PRODUCED_TO_KAFKA,
    /**
     * The consumer record has been put into drainer queue; the following cases will result in putting to drainer directly:
     * 1. Online/Offline ingestion task
     * 2. Follower replicas
     * 3. Leader is consuming from local version topics
     */
    QUEUED_TO_DRAINER,
    /**
     * The consumer record is skipped. e.g. remote VT's TS message during data recovery.
     */
    SKIPPED_MESSAGE
  }

  private DelegateConsumerRecordResult delegateConsumerRecordMaybeWithLock(
      PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingPerRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs) {
    if (!storeIngestionTask.isActiveActiveReplicationEnabled()
        || !consumerRecordWrapper.getMessage().getTopicPartition().getPubSubTopic().isRealTime()) {
      /**
       * We don't need to lock the partition here because during VT consumption there is only one consumption source.
       */
      return delegateConsumerRecord(
          consumerRecordWrapper,
          partition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingPerRecordTimestampNs,
          beforeProcessingBatchRecordsTimestampMs);
    } else {
      /**
       * The below flow must be executed in a critical session for the same key:
       * Read existing value/RMD from transient record cache/disk -> perform DCR and decide incoming value wins
       * -> update transient record cache -> produce to VT (just call send, no need to wait for the produce future in the critical session)
       *
       * Otherwise, there could be race conditions:
       * [fabric A thread]Read from transient record cache -> [fabric A thread]perform DCR and decide incoming value wins
       * -> [fabric B thread]read from transient record cache -> [fabric B thread]perform DCR and decide incoming value wins
       * -> [fabric B thread]update transient record cache -> [fabric B thread]produce to VT -> [fabric A thread]update transient record cache
       * -> [fabric A thread]produce to VT
       */
      final ByteArrayKey byteArrayKey = ByteArrayKey.wrap(consumerRecordWrapper.getMessage().getKey().getKey());
      ReentrantLock keyLevelLock = storeIngestionTask.getKeyLevelLocksManager().get().acquireLockByKey(byteArrayKey);
      keyLevelLock.lock();
      try {
        return delegateConsumerRecord(
            consumerRecordWrapper,
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingPerRecordTimestampNs,
            beforeProcessingBatchRecordsTimestampMs);
      } finally {
        keyLevelLock.unlock();
        storeIngestionTask.getKeyLevelLocksManager().get().releaseLock(byteArrayKey);
      }
    }
  }

  /**
   * The goal of this function is to possibly produce the incoming kafka message consumed from local VT, remote VT, RT or SR topic to
   * local VT if needed. It's decided based on the function output of {@link StoreIngestionTask#shouldProduceToVersionTopic} and message type.
   * It also perform any necessary additional computation operation such as for write-compute/update message.
   * It returns a boolean indicating if it was produced to kafka or not.
   *
   * This function should be called as one of the first steps in processing pipeline for all messages consumed from any kafka topic.
   *
   * The caller of this function should only process this {@param consumerRecord} further if the return is
   * {@link DelegateConsumerRecordResult#QUEUED_TO_DRAINER}.
   *
   * This function assumes {@link LeaderFollowerStoreIngestionTask#shouldProcessRecord(PubSubMessage)} has been called which happens in
   * {@link StorePartitionDataReceiver#produceToStoreBufferServiceOrKafka(Iterable, PubSubTopicPartition, String, int)}
   * before calling this and the it was decided that this record needs to be processed. It does not perform any
   * validation check on the PartitionConsumptionState object to keep the goal of the function simple and not overload.
   *
   * Also DIV validation is done here if the message is received from RT topic. For more info please see
   * please see {@link StoreIngestionTask#internalProcessConsumerRecord}
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this function.
   *
   * @return a {@link DelegateConsumerRecordResult} indicating what to do with the record
   */
  private DelegateConsumerRecordResult delegateConsumerRecord(
      PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingPerRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs) {
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord = consumerRecordWrapper.getMessage();
    try {
      KafkaKey kafkaKey = consumerRecord.getKey();
      KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
      /**
       * partitionConsumptionState must be in a valid state and no error reported. This is made sure by calling
       * {@link shouldProcessRecord} before processing any record.
       *
       * ^ This is no longer true because with shared consumer the partitionConsumptionState could have been removed
       * from unsubscribe action in the StoreIngestionTask thread. Today, when unsubscribing
       * {@link StoreIngestionTask.waitForAllMessageToBeProcessedFromTopicPartition} only ensure the buffer queue is
       * drained before unsubscribe. Records being processed by shared consumer may see invalid partitionConsumptionState.
       */
      PartitionConsumptionState partitionConsumptionState = storeIngestionTask.getPartitionConsumptionState(partition);
      if (partitionConsumptionState == null) {
        // The partition is likely unsubscribed, will skip these messages.
        return DelegateConsumerRecordResult.SKIPPED_MESSAGE;
      }
      boolean shouldProduceToLocalKafka = storeIngestionTask.shouldProduceToVersionTopic(partitionConsumptionState);
      // UPDATE message is only expected in LEADER which must be produced to kafka.
      MessageType msgType = MessageType.valueOf(kafkaValue);
      if (msgType == MessageType.UPDATE && !shouldProduceToLocalKafka) {
        throw new VeniceMessageException(
            storeIngestionTask.getIngestionTaskName()
                + " hasProducedToKafka: Received UPDATE message in non-leader for: "
                + consumerRecord.getTopicPartition() + " Offset " + consumerRecord.getOffset());
      } else if (msgType == MessageType.CONTROL_MESSAGE) {
        ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
        storeIngestionTask.getAndUpdateLeaderCompletedState(
            kafkaKey,
            kafkaValue,
            controlMessage,
            consumerRecord.getPubSubMessageHeaders(),
            partitionConsumptionState);
      }

      /**
       * return early if it needs not be produced to local VT such as cases like
       * (i) it's a follower or (ii) leader is consuming from VT
       */
      if (!shouldProduceToLocalKafka) {
        /**
         * For the local consumption, the batch data won't be produce to the local VT again, so we will switch
         * to real-time writer upon EOP here and for the remote consumption of VT, the switch will be handled
         * in the following section as it needs to flush the messages and then switch.
         */
        if (LeaderFollowerStoreIngestionTask.isLeader(partitionConsumptionState)
            && msgType == MessageType.CONTROL_MESSAGE
            && ControlMessageType.valueOf((ControlMessage) kafkaValue.payloadUnion).equals(END_OF_PUSH)) {
          LOGGER.info(
              "Switching to the VeniceWriter for real-time workload for topic: {}, partition: {}",
              storeIngestionTask.getVersionTopic().getName(),
              partition);
          // Just to be extra safe
          partitionConsumptionState.getVeniceWriterLazyRef().ifPresent(vw -> vw.flush());
          storeIngestionTask.setRealTimeVeniceWriterRef(partitionConsumptionState);
        }
        return DelegateConsumerRecordResult.QUEUED_TO_DRAINER;
      }

      // If we are here the message must be produced to local kafka or silently consumed.
      LeaderProducedRecordContext leaderProducedRecordContext;
      // No need to resolve cluster id and kafka url because sep topics are real time topic and it's not VT
      validateRecordBeforeProducingToLocalKafka(consumerRecord, partitionConsumptionState, kafkaUrl, kafkaClusterId);

      if (consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
        recordRegionHybridConsumptionStats(
            // convert the cluster id back to the original cluster id for monitoring purpose
            storeIngestionTask.getServerConfig()
                .getEquivalentKafkaClusterIdForSepTopic(
                    storeIngestionTask.getServerConfig().getEquivalentKafkaClusterIdForSepTopic(kafkaClusterId)),
            consumerRecord.getPayloadSize(),
            consumerRecord.getOffset(),
            beforeProcessingBatchRecordsTimestampMs);
        storeIngestionTask.updateLatestInMemoryLeaderConsumedRTOffset(
            partitionConsumptionState,
            kafkaUrl,
            consumerRecord.getOffset());
      }

      // heavy leader processing starts here
      storeIngestionTask.getVersionIngestionStats()
          .recordLeaderPreprocessingLatency(
              storeIngestionTask.getStoreName(),
              storeIngestionTask.getVersionNumber(),
              LatencyUtils.getElapsedTimeFromNSToMS(beforeProcessingPerRecordTimestampNs),
              beforeProcessingBatchRecordsTimestampMs);

      if (kafkaKey.isControlMessage()) {
        boolean producedFinally = true;
        ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
        ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
        leaderProducedRecordContext = LeaderProducedRecordContext
            .newControlMessageRecord(kafkaClusterId, consumerRecord.getOffset(), kafkaKey.getKey(), controlMessage);
        switch (controlMessageType) {
          case START_OF_PUSH:
            /**
             * N.B.: This is expected to be the first time we call {@link veniceWriter#get()}. During this time
             *       since startOfPush has not been processed yet, {@link StoreVersionState} is not created yet (unless
             *       this is a server restart scenario). So the {@link com.linkedin.venice.writer.VeniceWriter#isChunkingEnabled} field
             *       will not be set correctly at this point. This is fine as chunking is mostly not applicable for control messages.
             *       This chunking flag for the veniceWriter will happen be set correctly in
             *       {@link StoreIngestionTask#processStartOfPush(ControlMessage, int, long, PartitionConsumptionState)},
             *       which will be called when this message gets processed in drainer thread after successfully producing
             *       to kafka.
             *
             * Note update: the first time we call {@link veniceWriter#get()} is different in various use cases:
             * 1. For hybrid store with L/F enabled, the first time a VeniceWriter is created is after leader switches to RT and
             *    consumes the first message; potential message type: SOS, EOS, data message.
             * 2. For store version generated by stream reprocessing push type, the first time is after leader switches to
             *    SR topic and consumes the first message; potential message type: SOS, EOS, data message (consider server restart).
             * 3. For store with native replication enabled, the first time is after leader switches to remote topic and start
             *    consumes the first message; potential message type: SOS, EOS, SOP, EOP, data message (consider server restart).
             */
          case END_OF_PUSH:
            /**
             * Simply produce this EOP to local VT. It will be processed in order in the drainer queue later
             * after successfully producing to kafka.
             */
            produceToLocalKafka(
                consumerRecord,
                partitionConsumptionState,
                leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> partitionConsumptionState.getVeniceWriterLazyRef()
                    .get()
                    .put(
                        consumerRecord.getKey(),
                        consumerRecord.getValue(),
                        callback,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        leaderMetadataWrapper),
                partition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingPerRecordTimestampNs);
            partitionConsumptionState.getVeniceWriterLazyRef().get().flush();
            // Switch the writer for real-time workload
            LOGGER.info(
                "Switching to the VeniceWriter for real-time workload for topic: {}, partition: {}",
                storeIngestionTask.getVersionTopic().getName(),
                partition);
            storeIngestionTask.setRealTimeVeniceWriterRef(partitionConsumptionState);
            break;
          case START_OF_SEGMENT:
          case END_OF_SEGMENT:
            /**
             * SOS and EOS will be produced to the local version topic with DIV pass-through mode by leader in the following cases:
             * 1. SOS and EOS are from stream-reprocessing topics (use cases: stream-reprocessing)
             * 2. SOS and EOS are from version topics in a remote fabric (use cases: native replication for remote fabrics)
             *
             * SOS and EOS will not be produced to local version topic in the following cases:
             * 1. SOS and EOS are from real-time topics (use cases: hybrid ingestion, incremental push to RT)
             * 2. SOS and EOS are from version topics in local fabric, which has 2 different scenarios:
             *    i.  native replication is enabled, but the current fabric is the source fabric (use cases: native repl for source fabric)
             *    ii. native replication is not enabled; it doesn't matter whether current replica is leader or follower,
             *        messages from local VT doesn't need to be reproduced into local VT again (use case: local batch consumption)
             *
             * There is one exception that overrules the above conditions. i.e. if the SOS is a heartbeat from the RT topic.
             * In such case the heartbeat is produced to VT with updated {@link LeaderMetadataWrapper}.
             */
            if (!consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
              produceToLocalKafka(
                  consumerRecord,
                  partitionConsumptionState,
                  leaderProducedRecordContext,
                  (callback, leaderMetadataWrapper) -> partitionConsumptionState.getVeniceWriterLazyRef()
                      .get()
                      .put(
                          consumerRecord.getKey(),
                          consumerRecord.getValue(),
                          callback,
                          consumerRecord.getTopicPartition().getPartitionNumber(),
                          leaderMetadataWrapper),
                  partition,
                  kafkaUrl,
                  kafkaClusterId,
                  beforeProcessingPerRecordTimestampNs);
            } else {
              if (controlMessageType == ControlMessageType.START_OF_SEGMENT
                  && Arrays.equals(consumerRecord.getKey().getKey(), KafkaKey.HEART_BEAT.getKey())) {
                propagateHeartbeatFromUpstreamTopicToLocalVersionTopic(
                    partitionConsumptionState,
                    consumerRecord,
                    leaderProducedRecordContext,
                    partition,
                    kafkaUrl,
                    kafkaClusterId,
                    beforeProcessingPerRecordTimestampNs);
              } else {
                /**
                 * Based on current design handling this case (specially EOS) is tricky as we don't produce the SOS/EOS
                 * received from RT to local VT. But ideally EOS must be queued in-order (after all previous data message
                 * PUT/UPDATE/DELETE) to drainer. But since the queueing of data message to drainer
                 * happens in Kafka producer callback there is no way to queue this EOS to drainer in-order.
                 *
                 * Usually following processing in Leader for other control message.
                 *    1. DIV:
                 *    2. updateOffset:
                 *    3. stats maintenance as in {@link StoreIngestionTask#processKafkaDataMessage}
                 *
                 * For #1 Since we have moved the DIV validation in this function, We are good with DIV part which is the most critical one.
                 * For #2 Leader will not update the offset for SOS/EOS. From Server restart point of view this is tolerable. This was the case in previous design also. So there is no change in behaviour.
                 * For #3 stat counter update will not happen for SOS/EOS message. This should not be a big issue. If needed we can copy some of the stats maintenance
                 *   work here.
                 *
                 * So in summary NO further processing is needed SOS/EOS received from RT topics. Just silently drop the message here.
                 * We should not return false here.
                 */
                producedFinally = false;
              }
            }
            break;
          case START_OF_INCREMENTAL_PUSH:
          case END_OF_INCREMENTAL_PUSH:
            // For inc push to RT policy, the control msg is written in RT topic, we will need to calculate the
            // destination partition in VT correctly.
            int versionTopicPartitionToBeProduced = consumerRecord.getTopicPartition().getPartitionNumber();
            /**
             * We are using {@link VeniceWriter#asyncSendControlMessage()} api instead of {@link VeniceWriter#put()} since we have
             * to calculate DIV for this message but keeping the ControlMessage content unchanged. {@link VeniceWriter#put()} does not
             * allow that.
             */
            produceToLocalKafka(
                consumerRecord,
                partitionConsumptionState,
                leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> partitionConsumptionState.getVeniceWriterLazyRef()
                    .get()
                    .asyncSendControlMessage(
                        controlMessage,
                        versionTopicPartitionToBeProduced,
                        new HashMap<>(),
                        callback,
                        leaderMetadataWrapper),
                partition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingPerRecordTimestampNs);
            break;
          case TOPIC_SWITCH:
            /**
             * For TOPIC_SWITCH message we should use -1 as consumedOffset. This will ensure that it does not update the
             * setLeaderUpstreamOffset in:
             * {@link #updateOffsetsAsRemoteConsumeLeader(PartitionConsumptionState, LeaderProducedRecordContext, String, PubSubMessage, LeaderFollowerStoreIngestionTask.UpdateVersionTopicOffset, LeaderFollowerStoreIngestionTask.UpdateUpstreamTopicOffset)}
             * The leaderUpstreamOffset is set from the TS message config itself. We should not override it.
             */
            if (storeIngestionTask.isDataRecovery() && !partitionConsumptionState.isBatchOnly()) {
              // Ignore remote VT's TS message since we might need to consume more RT or incremental push data from VT
              // that's no longer in the local/remote RT due to retention.
              return DelegateConsumerRecordResult.SKIPPED_MESSAGE;
            }
            leaderProducedRecordContext =
                LeaderProducedRecordContext.newControlMessageRecord(kafkaKey.getKey(), controlMessage);
            produceToLocalKafka(
                consumerRecord,
                partitionConsumptionState,
                leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> partitionConsumptionState.getVeniceWriterLazyRef()
                    .get()
                    .asyncSendControlMessage(
                        controlMessage,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        new HashMap<>(),
                        callback,
                        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER),
                partition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingPerRecordTimestampNs);
            break;
          case VERSION_SWAP:
            return DelegateConsumerRecordResult.QUEUED_TO_DRAINER;
          default:
            // do nothing
            break;
        }
        if (!storeIngestionTask.isSegmentControlMsg(controlMessageType)) {
          LOGGER.info(
              "Replica: {} hasProducedToKafka: {}; ControlMessage: {}; Incoming record topic-partition: {}; offset: {}",
              partitionConsumptionState.getReplicaId(),
              producedFinally,
              controlMessageType.name(),
              consumerRecord.getTopicPartition(),
              consumerRecord.getOffset());
        }
      } else if (kafkaValue == null) {
        throw new VeniceMessageException(
            partitionConsumptionState.getReplicaId()
                + " hasProducedToKafka: Given null Venice Message. TopicPartition: "
                + consumerRecord.getTopicPartition() + " Offset " + consumerRecord.getOffset());
      } else {
        // This function may modify the original record in KME and it is unsafe to use the payload from KME directly
        // after this call.
        processMessageAndMaybeProduceToKafka(
            consumerRecordWrapper,
            partitionConsumptionState,
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingPerRecordTimestampNs,
            beforeProcessingBatchRecordsTimestampMs);
      }
      return DelegateConsumerRecordResult.PRODUCED_TO_KAFKA;
    } catch (Exception e) {
      throw new VeniceException(
          storeIngestionTask.getIngestionTaskName() + " hasProducedToKafka: exception for message received from: "
              + consumerRecord.getTopicPartition() + ", Offset: " + consumerRecord.getOffset() + ". Bubbling up.",
          e);
    }
  }

  /**
   * Checks before producing local version topic.
   *
   * Extend this function when there is new check needed.
   */
  void processMessageAndMaybeProduceToKafka(
      PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      PartitionConsumptionState partitionConsumptionState,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs) {
    /**
     * With {@link BatchConflictResolutionPolicy.BATCH_WRITE_LOSES} there is no need
     * to perform DCR before EOP and L/F DIV passthrough mode should be used. If the version is going through data
     * recovery then there is no need to perform DCR until we completed data recovery and switched to consume from RT.
     * TODO. We need to refactor this logic when we support other batch conflict resolution policy.
     */
    if (storeIngestionTask.isActiveActiveReplicationEnabled() && partitionConsumptionState.isEndOfPushReceived()
        && (!storeIngestionTask.isDataRecovery() || partitionConsumptionState.getTopicSwitch() == null)) {
      processActiveActiveMessageAndMaybeProduceToKafka(
          consumerRecordWrapper,
          partitionConsumptionState,
          partition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs,
          beforeProcessingBatchRecordsTimestampMs);
      return;
    }

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord = consumerRecordWrapper.getMessage();
    KafkaKey kafkaKey = consumerRecord.getKey();
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    byte[] keyBytes = kafkaKey.getKey();
    MessageType msgType = MessageType.valueOf(kafkaValue.messageType);

    WriteComputeResultWrapper writeComputeResultWrapper;
    if (consumerRecordWrapper.getProcessedResult() != null
        && consumerRecordWrapper.getProcessedResult().getWriteComputeResultWrapper() != null) {
      writeComputeResultWrapper = consumerRecordWrapper.getProcessedResult().getWriteComputeResultWrapper();
    } else {
      writeComputeResultWrapper =
          storeIngestionTask
              .processMessage(
                  consumerRecord,
                  partitionConsumptionState,
                  partition,
                  kafkaUrl,
                  kafkaClusterId,
                  beforeProcessingRecordTimestampNs,
                  beforeProcessingBatchRecordsTimestampMs)
              .getWriteComputeResultWrapper();
    }

    Put newPut = writeComputeResultWrapper.getNewPut();
    LeaderProducedRecordContext leaderProducedRecordContext;
    switch (msgType) {
      case PUT:
        leaderProducedRecordContext =
            LeaderProducedRecordContext.newPutRecord(kafkaClusterId, consumerRecord.getOffset(), keyBytes, newPut);
        produceToLocalKafka(
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            (callback, leaderMetadataWrapper) -> {
              /**
               * 1. Unfortunately, Kafka does not support fancy array manipulation via {@link ByteBuffer} or otherwise,
               * so we may be forced to do a copy here, if the backing array of the {@link putValue} has padding,
               * which is the case when using {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer}.
               * Since this is in a closure, it is not guaranteed to be invoked.
               *
               * The {@link OnlineOfflineStoreIngestionTask}, which ignores this closure, will not pay this price.
               *
               * Conversely, the {@link LeaderFollowerStoreIngestionTask}, which does invoke it, will.
               *
               * TODO: Evaluate holistically what is the best way to optimize GC for the L/F case.
               *
               * 2. Enable venice writer "pass-through" mode if we haven't received EOP yet. In pass through mode,
               * Leader will reuse upstream producer metadata. This would secures the correctness of DIV states in
               * followers when the leadership failover happens.
               */

              if (!partitionConsumptionState.isEndOfPushReceived()) {
                partitionConsumptionState.getVeniceWriterLazyRef()
                    .get()
                    .put(
                        kafkaKey,
                        kafkaValue,
                        callback,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        leaderMetadataWrapper);
              } else {
                partitionConsumptionState.getVeniceWriterLazyRef()
                    .get()
                    .put(
                        keyBytes,
                        ByteUtils.extractByteArray(newPut.putValue),
                        newPut.schemaId,
                        callback,
                        leaderMetadataWrapper);
              }
            },
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs);
        break;

      case UPDATE:
        if (writeComputeResultWrapper.isSkipProduce()) {
          return;
        }

        leaderProducedRecordContext =
            LeaderProducedRecordContext.newPutRecord(kafkaClusterId, consumerRecord.getOffset(), keyBytes, newPut);
        BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceFunction =
            (callback, leaderMetadataWrapper) -> partitionConsumptionState.getVeniceWriterLazyRef()
                .get()
                .put(
                    keyBytes,
                    ByteUtils.extractByteArray(newPut.getPutValue()),
                    newPut.getSchemaId(),
                    callback,
                    leaderMetadataWrapper,
                    APP_DEFAULT_LOGICAL_TS,
                    null,
                    writeComputeResultWrapper.getOldValueManifest(),
                    null);

        produceToLocalKafka(
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            produceFunction,
            partitionConsumptionState.getPartition(),
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs);
        break;

      case DELETE:
        leaderProducedRecordContext = LeaderProducedRecordContext
            .newDeleteRecord(kafkaClusterId, consumerRecord.getOffset(), keyBytes, (Delete) kafkaValue.payloadUnion);
        produceToLocalKafka(
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            (callback, leaderMetadataWrapper) -> {
              /**
               * DIV pass-through for DELETE messages before EOP.
               */
              if (!partitionConsumptionState.isEndOfPushReceived()) {
                partitionConsumptionState.getVeniceWriterLazyRef()
                    .get()
                    .delete(
                        kafkaKey,
                        kafkaValue,
                        callback,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        leaderMetadataWrapper);
              } else {
                partitionConsumptionState.getVeniceWriterLazyRef()
                    .get()
                    .delete(keyBytes, callback, leaderMetadataWrapper);
              }
            },
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs);
        break;

      default:
        throw new VeniceMessageException(
            storeIngestionTask.getIngestionTaskName() + " : Invalid/Unrecognized operation type submitted: "
                + kafkaValue.messageType);
    }
  }

  // This function may modify the original record in KME, it is unsafe to use the payload from KME directly after
  // this function.
  private void processActiveActiveMessageAndMaybeProduceToKafka(
      PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      PartitionConsumptionState partitionConsumptionState,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs) {
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord = consumerRecordWrapper.getMessage();
    KafkaKey kafkaKey = consumerRecord.getKey();
    byte[] keyBytes = kafkaKey.getKey();
    final MergeConflictResultWrapper mergeConflictResultWrapper;
    if (consumerRecordWrapper.getProcessedResult() != null
        && consumerRecordWrapper.getProcessedResult().getMergeConflictResultWrapper() != null) {
      mergeConflictResultWrapper = consumerRecordWrapper.getProcessedResult().getMergeConflictResultWrapper();
    } else {
      mergeConflictResultWrapper =
          storeIngestionTask
              .processActiveActiveMessage(
                  consumerRecord,
                  partitionConsumptionState,
                  partition,
                  kafkaUrl,
                  kafkaClusterId,
                  beforeProcessingRecordTimestampNs,
                  beforeProcessingBatchRecordsTimestampMs)
              .getMergeConflictResultWrapper();
    }

    MergeConflictResult mergeConflictResult = mergeConflictResultWrapper.getMergeConflictResult();
    if (!mergeConflictResult.isUpdateIgnored()) {
      // Apply this update to any views for this store
      // TODO: It'd be good to be able to do this in LeaderFollowerStoreIngestionTask instead, however, AA currently is
      // the
      // only extension of IngestionTask which does a read from disk before applying the record. This makes the
      // following function
      // call in this context much less obtrusive, however, it implies that all views can only work for AA stores

      // Write to views
      final Map<String, VeniceViewWriter> viewWriters = storeIngestionTask.getViewWriters();
      if (!viewWriters.isEmpty()) {
        /**
         * The ordering guarantees we want is the following:
         *
         * 1. Write to all view topics (in parallel).
         * 2. Write to the VT only after we get the ack for all views AND the previous write to VT was queued into the
         *    producer (but not necessarily acked).
         */
        long preprocessingTime = System.currentTimeMillis();
        CompletableFuture currentVersionTopicWrite = new CompletableFuture();
        CompletableFuture[] viewWriterFutures = new CompletableFuture[viewWriters.size() + 1];
        int index = 0;
        // The first future is for the previous write to VT
        viewWriterFutures[index++] = partitionConsumptionState.getLastVTProduceCallFuture();
        ByteBuffer oldValueBB = mergeConflictResultWrapper.getOldValueByteBufferProvider().get();
        int oldValueSchemaId =
            oldValueBB == null ? -1 : mergeConflictResultWrapper.getOldValueProvider().get().writerSchemaId();
        for (VeniceViewWriter writer: viewWriters.values()) {
          viewWriterFutures[index++] = writer.processRecord(
              mergeConflictResult.getNewValue(),
              oldValueBB,
              keyBytes,
              storeIngestionTask.getVersionNumber(),
              mergeConflictResult.getValueSchemaId(),
              oldValueSchemaId,
              mergeConflictResult.getRmdRecord());
        }
        CompletableFuture.allOf(viewWriterFutures).whenCompleteAsync((value, exception) -> {
          storeIngestionTask.getHostLevelIngestionStats()
              .recordViewProducerLatency(LatencyUtils.getElapsedTimeFromMsToMs(preprocessingTime));
          if (exception == null) {
            producePutOrDeleteToKafka(
                mergeConflictResultWrapper,
                partitionConsumptionState,
                keyBytes,
                consumerRecord,
                partition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingRecordTimestampNs);
            currentVersionTopicWrite.complete(null);
          } else {
            VeniceException veniceException = new VeniceException(exception);
            storeIngestionTask.setIngestionException(partitionConsumptionState.getPartition(), veniceException);
            currentVersionTopicWrite.completeExceptionally(veniceException);
          }
        });
        partitionConsumptionState.setLastVTProduceCallFuture(currentVersionTopicWrite);
      } else {
        // This function may modify the original record in KME and it is unsafe to use the payload from KME directly
        // after
        // this call.
        producePutOrDeleteToKafka(
            mergeConflictResultWrapper,
            partitionConsumptionState,
            keyBytes,
            consumerRecord,
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs);
      }
    }
  }

  /**
   * This function parses the {@link MergeConflictResult} and decides if the update should be ignored or emit a PUT or a
   * DELETE record to VT.
   * <p>
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after
   * this function.
   *
   * @param mergeConflictResultWrapper       The result of conflict resolution.
   * @param partitionConsumptionState The {@link PartitionConsumptionState} of the current partition
   * @param key                       The key bytes of the incoming record.
   * @param consumerRecord            The {@link PubSubMessage} for the current record.
   * @param partition
   * @param kafkaUrl
   */
  private void producePutOrDeleteToKafka(
      MergeConflictResultWrapper mergeConflictResultWrapper,
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs) {
    MergeConflictResult mergeConflictResult = mergeConflictResultWrapper.getMergeConflictResult();
    ByteBuffer updatedValueBytes = mergeConflictResultWrapper.getUpdatedValueBytes();
    ByteBuffer updatedRmdBytes = mergeConflictResultWrapper.getUpdatedRmdBytes();
    final int valueSchemaId = mergeConflictResult.getValueSchemaId();

    ChunkedValueManifest oldValueManifest = mergeConflictResultWrapper.getOldValueManifestContainer().getManifest();
    ChunkedValueManifest oldRmdManifest = mergeConflictResultWrapper.getOldRmdWithValueSchemaId() == null
        ? null
        : mergeConflictResultWrapper.getOldRmdWithValueSchemaId().getRmdManifest();
    // finally produce
    if (mergeConflictResultWrapper.getUpdatedValueBytes() == null) {
      storeIngestionTask.getHostLevelIngestionStats().recordTombstoneCreatedDCR();
      storeIngestionTask.getAggVersionedIngestionStats()
          .recordTombStoneCreationDCR(storeIngestionTask.getStoreName(), storeIngestionTask.getVersionNumber());
      Delete deletePayload = new Delete();
      deletePayload.schemaId = valueSchemaId;
      deletePayload.replicationMetadataVersionId = storeIngestionTask.getRmdProtocolVersionId();
      deletePayload.replicationMetadataPayload = mergeConflictResultWrapper.getUpdatedRmdBytes();
      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceToTopicFunction =
          (callback, sourceTopicOffset) -> partitionConsumptionState.getVeniceWriterLazyRef()
              .get()
              .delete(
                  key,
                  callback,
                  sourceTopicOffset,
                  VeniceWriter.APP_DEFAULT_LOGICAL_TS,
                  new DeleteMetadata(valueSchemaId, storeIngestionTask.getRmdProtocolVersionId(), updatedRmdBytes),
                  oldValueManifest,
                  oldRmdManifest);
      LeaderProducedRecordContext leaderProducedRecordContext =
          LeaderProducedRecordContext.newDeleteRecord(kafkaClusterId, consumerRecord.getOffset(), key, deletePayload);
      produceToLocalKafka(
          consumerRecord,
          partitionConsumptionState,
          leaderProducedRecordContext,
          produceToTopicFunction,
          partition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs);
    } else {
      Put updatedPut = new Put();
      updatedPut.putValue = ByteUtils
          .prependIntHeaderToByteBuffer(updatedValueBytes, valueSchemaId, mergeConflictResult.doesResultReuseInput());
      updatedPut.schemaId = valueSchemaId;
      updatedPut.replicationMetadataVersionId = storeIngestionTask.getRmdProtocolVersionId();
      updatedPut.replicationMetadataPayload = updatedRmdBytes;

      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceToTopicFunction = getProduceToTopicFunction(
          partitionConsumptionState,
          key,
          updatedValueBytes,
          updatedRmdBytes,
          oldValueManifest,
          oldRmdManifest,
          valueSchemaId,
          mergeConflictResult.doesResultReuseInput());
      produceToLocalKafka(
          consumerRecord,
          partitionConsumptionState,
          LeaderProducedRecordContext.newPutRecord(kafkaClusterId, consumerRecord.getOffset(), key, updatedPut),
          produceToTopicFunction,
          partition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs);
    }
  }

  BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> getProduceToTopicFunction(
      PartitionConsumptionState partitionConsumptionState,
      byte[] key,
      ByteBuffer updatedValueBytes,
      ByteBuffer updatedRmdBytes,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest,
      int valueSchemaId,
      boolean resultReuseInput) {
    return (callback, leaderMetadataWrapper) -> {
      if (resultReuseInput) {
        // Restore the original header so this function is eventually idempotent as the original KME ByteBuffer
        // will be recovered after producing the message to Kafka or if the production failing.
        ((ActiveActiveProducerCallback) callback).setOnCompletionFunction(
            () -> ByteUtils.prependIntHeaderToByteBuffer(
                updatedValueBytes,
                ByteUtils.getIntHeaderFromByteBuffer(updatedValueBytes),
                true));
      }
      storeIngestionTask.getVeniceWriter(partitionConsumptionState)
          .get()
          .put(
              key,
              ByteUtils.extractByteArray(updatedValueBytes),
              valueSchemaId,
              callback,
              leaderMetadataWrapper,
              VeniceWriter.APP_DEFAULT_LOGICAL_TS,
              new PutMetadata(storeIngestionTask.getRmdProtocolVersionId(), updatedRmdBytes),
              oldValueManifest,
              oldRmdManifest);
    };
  }

  private void validateRecordBeforeProducingToLocalKafka(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId) {
    // Check whether the message is from local version topic; leader shouldn't consume from local VT and then produce
    // back to VT again
    // localKafkaClusterId will always be the regular one without "_sep" suffix so kafkaClusterId should be converted
    // for comparison. Like-wise for the kafkaUrl.
    if (kafkaClusterId == storeIngestionTask.getLocalKafkaClusterId()
        && consumerRecord.getTopicPartition().getPubSubTopic().equals(storeIngestionTask.getVersionTopic())
        && kafkaUrl.equals(storeIngestionTask.getLocalKafkaServer())) {
      // N.B.: Ideally, the first two conditions should be sufficient, but for some reasons, in certain tests, the
      // third condition also ends up being necessary. In any case, doing the cluster ID check should be a
      // fast short-circuit in normal cases.
      try {
        int partitionId = partitionConsumptionState.getPartition();
        storeIngestionTask.setIngestionException(
            partitionId,
            new VeniceException(
                "Store version " + storeIngestionTask.getVersionTopic() + " partition " + partitionId
                    + " is consuming from local version topic and producing back to local version topic"
                    + ", kafkaClusterId = " + kafkaClusterId + ", kafkaUrl = " + kafkaUrl + ", this.localKafkaServer = "
                    + storeIngestionTask.getLocalKafkaServer()));
      } catch (VeniceException offerToQueueException) {
        storeIngestionTask.setLastStoreIngestionException(offerToQueueException);
      }
    }
  }

  void produceToLocalKafka(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceFunction,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs) {
    LeaderProducerCallback callback = createProducerCallback(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        partition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
    long sourceTopicOffset = consumerRecord.getOffset();
    LeaderMetadataWrapper leaderMetadataWrapper = new LeaderMetadataWrapper(sourceTopicOffset, kafkaClusterId);
    partitionConsumptionState.setLastLeaderPersistFuture(leaderProducedRecordContext.getPersistedToDBFuture());
    long beforeProduceTimestampNS = System.nanoTime();
    produceFunction.accept(callback, leaderMetadataWrapper);
    storeIngestionTask.getHostLevelIngestionStats()
        .recordLeaderProduceLatency(LatencyUtils.getElapsedTimeFromNSToMS(beforeProduceTimestampNS));

    // Update the partition consumption state to say that we've transmitted the message to kafka (but haven't
    // necessarily received an ack back yet).
    if (storeIngestionTask.isActiveActiveReplicationEnabled()
        && partitionConsumptionState.getLeaderFollowerState() == LeaderFollowerStateType.LEADER
        && partitionConsumptionState.isHybrid() && consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
      partitionConsumptionState.updateLatestRTOffsetTriedToProduceToVTMap(kafkaUrl, consumerRecord.getOffset());
    }
  }

  private void recordRegionHybridConsumptionStats(
      int kafkaClusterId,
      int producedRecordSize,
      long upstreamOffset,
      long currentTimeMs) {
    if (kafkaClusterId >= 0) {
      storeIngestionTask.getVersionIngestionStats()
          .recordRegionHybridConsumption(
              storeIngestionTask.getStoreName(),
              storeIngestionTask.getVersionNumber(),
              kafkaClusterId,
              producedRecordSize,
              upstreamOffset,
              currentTimeMs);
      storeIngestionTask.getHostLevelIngestionStats()
          .recordTotalRegionHybridBytesConsumed(kafkaClusterId, producedRecordSize, currentTimeMs);
    }
  }

  /**
   * Leaders propagate HB SOS message from RT to local VT (to all subpartitions in case if amplification
   * Factor is configured to be more than 1) with updated LeaderCompleteState header:
   * Adding the headers during this phase instead of adding it to RT directly simplifies the logic
   * of how to identify the HB SOS from the correct version or whether the HB SOS is from the local
   * colo or remote colo, as the header inherited from an incorrect version or remote colos might
   * provide incorrect information about the leader state.
   */
  private void propagateHeartbeatFromUpstreamTopicToLocalVersionTopic(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs) {
    LeaderProducerCallback callback = createProducerCallback(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        partition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
    LeaderMetadataWrapper leaderMetadataWrapper = new LeaderMetadataWrapper(consumerRecord.getOffset(), kafkaClusterId);
    LeaderCompleteState leaderCompleteState =
        LeaderCompleteState.getLeaderCompleteState(partitionConsumptionState.isCompletionReported());
    /**
     * The maximum value between the original producer timestamp and the timestamp when the message is added to the RT topic is used:
     * This approach addresses scenarios wrt clock drift where the producer's timestamp is consistently delayed by several minutes,
     * causing it not to align with the {@link VeniceServerConfig#getLeaderCompleteStateCheckInFollowerValidIntervalMs()}
     * interval. The likelihood of simultaneous significant time discrepancies between the leader (producer) and the RT should be very
     * rare, making this a viable workaround. In cases where the time discrepancy is reversed, the follower may complete slightly earlier
     * than expected. However, this should not pose a significant issue as the completion of the leader, indicated by the leader
     * completed header, is a prerequisite for the follower completion and is expected to occur shortly thereafter.
     */
    long producerTimeStamp =
        Long.max(consumerRecord.getPubSubMessageTime(), consumerRecord.getValue().producerMetadata.messageTimestamp);
    PubSubTopicPartition topicPartition =
        new PubSubTopicPartitionImpl(storeIngestionTask.getVersionTopic(), partitionConsumptionState.getPartition());
    sendIngestionHeartbeatToVT(
        partitionConsumptionState,
        topicPartition,
        callback,
        leaderMetadataWrapper,
        leaderCompleteState,
        producerTimeStamp);
  }

  private void sendIngestionHeartbeatToVT(
      PartitionConsumptionState partitionConsumptionState,
      PubSubTopicPartition topicPartition,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      LeaderCompleteState leaderCompleteState,
      long originTimeStampMs) {
    storeIngestionTask.sendIngestionHeartbeat(
        partitionConsumptionState,
        topicPartition,
        callback,
        leaderMetadataWrapper,
        true,
        true,
        leaderCompleteState,
        originTimeStampMs);
  }

  private LeaderProducerCallback createProducerCallback(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    return storeIngestionTask.isActiveActiveReplicationEnabled()
        ? new ActiveActiveProducerCallback(
            (ActiveActiveStoreIngestionTask) storeIngestionTask,
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            partition,
            kafkaUrl,
            beforeProcessingRecordTimestampNs)
        : new LeaderProducerCallback(
            (LeaderFollowerStoreIngestionTask) storeIngestionTask,
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            partition,
            kafkaUrl,
            beforeProcessingRecordTimestampNs);
  }

  @Override
  public PubSubTopic destinationIdentifier() {
    return storeIngestionTask.getVersionTopic();
  }

  @Override
  public void notifyOfTopicDeletion(String topicName) {
    storeIngestionTask.setLastConsumerException(new VeniceException("Topic " + topicName + " got deleted."));
  }

  private void handleDataReceiverException(Exception e) throws Exception {
    if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
      // We sometimes wrap InterruptedExceptions, so not taking any chances...
      if (storeIngestionTask.isRunning()) {
        /**
         * Based on the order of operations in {@link KafkaStoreIngestionService#stopInner()} the ingestion
         * tasks should all be closed (and therefore not running) prior to this service here being stopped.
         * Hence, the state detected here where we get interrupted while the ingestion task is still running
         * should never happen. It's unknown whether this happens or not, and if it does, whether it carries
         * any significant consequences. For now we will only log it if it does happen, but will not take
         * any special action. Some action which we might consider taking in the future would be to call
         * {@link StoreIngestionTask#close()} here, but in the interest of keeping the shutdown flow
         * simpler, we will avoid doing this for now.
         */
        LOGGER.warn(
            "Unexpected: got interrupted prior to the {} getting closed.",
            storeIngestionTask.getClass().getSimpleName());
      }
      /**
       * We want to rethrow the interrupted exception in order to skip the quota-related code below and
       * break the run loop. We avoid calling {@link StoreIngestionTask#setLastConsumerException(Exception)}
       * as we do for other exceptions as this carries side-effects that may be undesirable.
       */
      throw e;
    }
    LOGGER.error(
        "Received exception when StoreIngestionTask is processing the polled consumer record for topic: {}",
        topicPartition,
        e);
    storeIngestionTask.setLastConsumerException(e);
  }

  /**
   * @return Number of data records put in the receiver, for testing purpose.
   */
  public long receivedRecordsCount() {
    return receivedRecordsCount;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" + "VT=" + storeIngestionTask.getVersionTopic() + ", topicPartition="
        + topicPartition + '}';
  }

  // for testing purpose only
  int getKafkaClusterId() {
    return this.kafkaClusterId;
  }
}
