/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.ReadTargetDispatcher;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.kafka.activation.KafkaGenerationProtocolActivationGuard;
import com.nereusstream.kafka.activation.KafkaStorageActivationVerifier;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import com.nereusstream.kafka.runtime.NereusKafkaCompactionContext;
import com.nereusstream.kafka.runtime.NereusKafkaCompactionRuntimeConfiguration;
import com.nereusstream.materialization.DefaultCommittedSourceSetResolver;
import com.nereusstream.materialization.DefaultExactSourceRangeReader;
import com.nereusstream.materialization.DefaultGenerationCommitter;
import com.nereusstream.materialization.DefaultMaterializationTaskProtectionReconciler;
import com.nereusstream.materialization.DefaultMaterializationOutputVerifier;
import com.nereusstream.materialization.DefaultTerminalMaterializationSourceProtectionReleaser;
import com.nereusstream.materialization.MaterializationSourceProtectionAdapter;
import com.nereusstream.materialization.MaterializationSourceProtectionRegistry;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.materialization.MaterializationStreamAuthorityMode;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.materialization.ObjectMaterializationSourceProtectionAdapter;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedWriter;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/** Complete product-owned composition of the F9 Object-WAL compaction runtime. */
public final class KafkaCompactionProductionRuntimeFactory {
  private KafkaCompactionProductionRuntimeFactory() {}

  public static KafkaCompactionRuntime create(
      NereusKafkaCompactionContext context,
      String cluster,
      String processRunId,
      KafkaPartitionStorageManager partitions,
      OxiaMetadataStore l0Metadata,
      GenerationMetadataStore generations,
      PhysicalObjectMetadataStore physicalObjects,
      KafkaPartitionMetadataStore partitionMetadata,
      KafkaCompactionPlanMetadataStore plans,
      ObjectProtectionManager protections,
      ObjectReadPinManager readPins,
      ReadTargetReaderRegistry readers,
      List<MaterializationSourceProvider> additionalPrimarySources,
      ObjectStore objectStore,
      StagingFileManager stagingFiles,
      KafkaStorageActivationVerifier activationVerifier,
      ScheduledExecutorService scheduler,
      Executor callbackExecutor,
      Clock clock) {
    NereusKafkaCompactionContext exactContext =
        Objects.requireNonNull(context, "context");
    NereusKafkaCompactionRuntimeConfiguration configuration =
        exactContext.configuration();
    String exactCluster = requireText(cluster, "cluster");
    String exactProcessRunId = workerProcessRunId(processRunId);
    KafkaPartitionStorageManager exactPartitions =
        Objects.requireNonNull(partitions, "partitions");
    OxiaMetadataStore exactL0 = Objects.requireNonNull(l0Metadata, "l0Metadata");
    GenerationMetadataStore exactGenerations =
        Objects.requireNonNull(generations, "generations");
    PhysicalObjectMetadataStore exactPhysical =
        Objects.requireNonNull(physicalObjects, "physicalObjects");
    KafkaPartitionMetadataStore exactPartitionMetadata =
        Objects.requireNonNull(partitionMetadata, "partitionMetadata");
    KafkaCompactionPlanMetadataStore exactPlans = Objects.requireNonNull(plans, "plans");
    ObjectProtectionManager exactProtections =
        Objects.requireNonNull(protections, "protections");
    ObjectReadPinManager exactReadPins = Objects.requireNonNull(readPins, "readPins");
    ReadTargetReaderRegistry exactReaders = Objects.requireNonNull(readers, "readers");
    List<MaterializationSourceProvider> exactAdditionalSources =
        List.copyOf(
            Objects.requireNonNull(
                additionalPrimarySources, "additionalPrimarySources"));
    ObjectStore exactObjectStore = Objects.requireNonNull(objectStore, "objectStore");
    StagingFileManager exactStaging = Objects.requireNonNull(stagingFiles, "stagingFiles");
    KafkaStorageActivationVerifier exactActivation =
        Objects.requireNonNull(activationVerifier, "activationVerifier");
    ScheduledExecutorService exactScheduler = Objects.requireNonNull(scheduler, "scheduler");
    Executor exactCallbacks = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
    Clock exactClock = Objects.requireNonNull(clock, "clock");

    MaterializationTaskStore tasks =
        new MaterializationTaskStore(exactCluster, exactGenerations, exactClock);
    KafkaActivatedGenerationSetResolver activated =
        new KafkaActivatedGenerationSetResolver(exactCluster, exactGenerations);
    DefaultCommittedSourceSetResolver committedSources =
        new DefaultCommittedSourceSetResolver(
            exactCluster,
            exactL0,
            exactGenerations,
            configuration.metadataScanPageSize(),
            MaterializationStreamAuthorityMode.KAFKA_TOPIC_COMPACTION);
    KafkaCompactionSourceResolver sourceResolver =
        new KafkaCompactionSourceResolver(committedSources);
    MetadataPhysicalObjectIdentityResolver identities =
        new MetadataPhysicalObjectIdentityResolver(
            exactCluster, exactL0, exactPhysical);
    MaterializationSourceProtectionRegistry sourceProtections =
        sourceProtections(identities, exactProtections, exactAdditionalSources);
    DefaultMaterializationTaskProtectionReconciler taskProtections =
        new DefaultMaterializationTaskProtectionReconciler(
            exactCluster,
            tasks,
            exactGenerations,
            identities,
            exactProtections,
            sourceProtections,
            configuration.generationOperationTimeout(),
            exactScheduler);
    ReadTargetDispatcher dispatcher = new ReadTargetDispatcher(exactReaders);
    KafkaCompactionBatchSource batchSource =
        new KafkaCompactionBatchSource(
            streamId ->
                new DefaultExactSourceRangeReader(
                    exactCluster,
                    streamId,
                    exactGenerations,
                    identities,
                    exactReadPins,
                    dispatcher,
                    configuration.sourceReadPageRecords(),
                    configuration.sourceReadPageBytes(),
                    exactClock,
                    exactCallbacks),
            configuration.sourceReadOptions(),
            exactCallbacks);
    KafkaCompactionStreamingExecutor executor =
        new KafkaCompactionStreamingExecutor(
            new KafkaTopicCompactionCodecV1(),
            new KafkaCompactionStrategyV1(),
            new KafkaCompactionRowMapper(),
            configuration.executorLimits(),
            exactStaging,
            exactCallbacks);
    KafkaCompactionParquetPublisher parquet =
        new KafkaCompactionParquetPublisher(
            executor,
            new KafkaCompactionWriteRequestFactory(),
            new ParquetKafkaTopicCompactedWriter(exactStaging, exactCallbacks));
    RangedCompactedObjectVerifier objectVerifier =
        new RangedCompactedObjectVerifier(
            exactObjectStore,
            new ParquetRangedCompactedObjectReader(exactObjectStore, exactCallbacks),
            new ParquetKafkaTopicCompactedReader(exactObjectStore, exactCallbacks));
    DefaultGenerationCommitter generationCommitter =
        new DefaultGenerationCommitter(
            exactCluster,
            exactL0,
            exactGenerations,
            exactPhysical,
            exactProtections,
            sourceProtections,
            new KafkaGenerationProtocolActivationGuard(
                exactCluster,
                exactGenerations,
                exactL0,
                exactActivation,
                exactClock),
            new DefaultMaterializationOutputVerifier(
                exactObjectStore,
                new KafkaCompactionMaterializationFormatVerifier(objectVerifier)),
            MaterializationStreamAuthorityMode.KAFKA_TOPIC_COMPACTION,
            configuration.generationOperationTimeout(),
            exactScheduler,
            exactClock);
    KafkaCompactionPublicationCoordinator publications =
        new KafkaCompactionPublicationCoordinator(
            exactCluster,
            exactObjectStore,
            objectVerifier,
            tasks,
            generationCommitter,
            exactPartitionMetadata,
            configuration.generationOperationTimeout(),
            exactClock);
    KafkaCompactionPlanCoordinator planCoordinator =
        new KafkaCompactionPlanCoordinator(exactPlans, tasks, exactClock);
    KafkaCompactionTerminalRetirer retirer =
        new KafkaCompactionTerminalRetirer(
            exactPlans,
            tasks,
            new DefaultTerminalMaterializationSourceProtectionReleaser(
                exactCluster,
                tasks,
                sourceProtections,
                configuration.generationOperationTimeout(),
                exactScheduler));
    KafkaCompactionPlanner planner = new KafkaCompactionPlanner();

    return new KafkaCompactionRuntime(
        exactPartitions,
        exactContext.ownedPartitions(),
        owned -> {
          KafkaCompactionPartitionPass pass =
              new KafkaCompactionPartitionPass(
                  exactCluster,
                  owned.identity().durableId(),
                  owned.captureProvider(),
                  planner,
                  sourceResolver,
                  planCoordinator,
                  exactPlans,
                  tasks,
                  taskProtections,
                  batchSource,
                  parquet,
                  publications,
                  retirer,
                  activated,
                  exactProcessRunId,
                  configuration.partitionPass(),
                  exactScheduler,
                  exactClock);
          return ignored -> pass.runOnce();
        },
        configuration.interval(),
        configuration.maxConcurrentPartitions(),
        configuration.maxPartitionsPerPass(),
        exactScheduler,
        exactCallbacks);
  }

  private static MaterializationSourceProtectionRegistry sourceProtections(
      MetadataPhysicalObjectIdentityResolver identities,
      ObjectProtectionManager protections,
      List<MaterializationSourceProvider> additionalSources) {
    List<MaterializationSourceProtectionAdapter<?>> adapters = new ArrayList<>();
    adapters.add(
        new ObjectMaterializationSourceProtectionAdapter(
            identities, protections));
    additionalSources.stream()
        .map(MaterializationSourceProvider::protectionAdapter)
        .forEach(adapters::add);
    return new MaterializationSourceProtectionRegistry(adapters);
  }

  private static String requireText(String value, String field) {
    String exact = Objects.requireNonNull(value, field);
    if (exact.isBlank()) {
      throw new IllegalArgumentException(field + " cannot be blank");
    }
    return exact;
  }

  static String workerProcessRunId(String runtimeProcessId) {
    return DeterministicIds.stableHashComponent(
        requireText(runtimeProcessId, "runtimeProcessId"));
  }
}
