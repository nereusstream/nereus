/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV5;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.ActionExecutor;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.CampaignResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.HardDeadline;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.StopSignal;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.TerminalReason;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Objects;

/** V5 authority wrapper around the unchanged, validator-driven V3 logical orchestration algebra. */
final class M3V5AdaptiveCampaignExecutor {
    private static final RemainingBudgets LOGICAL_BUDGETS =
            new RemainingBudgets(900, 5_400, 7_200, 5_400, 13_120, 1_640, 600);

    private final SourceBinding source;
    private final ActionExecutor actions;
    private final CheckpointSink checkpoints;
    private final StopSignal stopSignal;
    private final HardDeadline hardDeadline;

    M3V5AdaptiveCampaignExecutor(
            SourceBinding source,
            ActionExecutor actions,
            CheckpointSink checkpoints,
            StopSignal stopSignal,
            HardDeadline hardDeadline) {
        this.source = Objects.requireNonNull(source, "source");
        if (!source.workloadDigest().equals(M3V5FormalCampaignPlan.zeroDecisionPlanDigest())) {
            throw new IllegalArgumentException("allocator V5 source binding differs from its zero-decision plan");
        }
        this.actions = Objects.requireNonNull(actions, "actions");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.stopSignal = Objects.requireNonNull(stopSignal, "stopSignal");
        this.hardDeadline = Objects.requireNonNull(hardDeadline, "hardDeadline");
    }

    Result start() throws IOException {
        SinkBridge bridge = new SinkBridge(checkpoints, null);
        CampaignResult logical = logicalExecutor(bridge).start();
        return bridge.result(logical);
    }

    Result resume(CanonicalBytes predecessorBytes) throws IOException {
        AllocatorCampaignCheckpointV5 predecessor =
                AllocatorCampaignCheckpointV5.decode(Objects.requireNonNull(predecessorBytes, "predecessorBytes"));
        if (!predecessor.source().equals(source)) {
            throw new IllegalArgumentException("allocator V5 resume source/profile tuple differs");
        }
        SinkBridge bridge = new SinkBridge(checkpoints, predecessorBytes);
        CampaignResult logical = logicalExecutor(bridge).resume(predecessor.logicalCheckpointBytes());
        return bridge.result(logical);
    }

    private M3V3AdaptiveCampaignExecutor logicalExecutor(SinkBridge bridge) {
        return new M3V3AdaptiveCampaignExecutor(
                source, LOGICAL_BUDGETS, actions, bridge, stopSignal, hardDeadline);
    }

    @FunctionalInterface
    interface CheckpointSink {
        void persist(long sequence, CanonicalBytes checkpointBytes) throws IOException;

        static CheckpointSink createNewDirectory(Path directory) {
            Path exactDirectory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
            return (sequence, checkpointBytes) -> {
                if (!Files.isDirectory(exactDirectory, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException("allocator V5 checkpoint directory is absent or a link");
                }
                String digest = AllocatorCampaignCheckpointV5.digest(checkpointBytes).toHex();
                Path output = exactDirectory.resolve(String.format(
                        Locale.ROOT, "checkpoint-%020d-%s.nacp5", sequence, digest));
                Files.write(
                        output,
                        checkpointBytes.toByteArray(),
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.DSYNC);
            };
        }
    }

    record Result(CanonicalBytes checkpointBytes, Status status, TerminalReason reason, String detail) {
        Result {
            Objects.requireNonNull(checkpointBytes, "checkpointBytes");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(detail, "detail");
            if (AllocatorCampaignCheckpointV5.decode(checkpointBytes).status() != status) {
                throw new IllegalArgumentException("allocator V5 campaign result status differs from checkpoint");
            }
        }

        boolean completed() {
            return status == Status.COMPLETED && reason == TerminalReason.COMPLETED;
        }
    }

    private static final class SinkBridge implements M3V3AdaptiveCampaignExecutor.CheckpointSink {
        private final CheckpointSink output;
        private CanonicalBytes previous;

        private SinkBridge(CheckpointSink output, CanonicalBytes previous) {
            this.output = output;
            this.previous = previous;
        }

        @Override
        public void persist(long sequence, CanonicalBytes logicalBytes) throws IOException {
            AllocatorCampaignCheckpointV5 checkpoint = previous == null
                    ? AllocatorCampaignCheckpointV5.initial(logicalBytes)
                    : AllocatorCampaignCheckpointV5.resume(previous, logicalBytes);
            CanonicalBytes encoded = AllocatorCampaignCheckpointV5.encode(checkpoint);
            if (checkpoint.checkpointSequence() != sequence) {
                throw new IllegalArgumentException("allocator V5 checkpoint sequence differs from logical executor");
            }
            output.persist(sequence, encoded);
            previous = encoded;
        }

        private Result result(CampaignResult logical) {
            if (previous == null) {
                throw new IllegalStateException("allocator V5 logical executor produced no checkpoint");
            }
            AllocatorCampaignCheckpointV5 checkpoint = AllocatorCampaignCheckpointV5.decode(previous);
            if (!checkpoint.logicalCheckpointBytes().equals(logical.checkpointBytes())) {
                throw new IllegalStateException("allocator V5 final logical checkpoint differs from persisted NACP5");
            }
            return new Result(previous, logical.status(), logical.reason(), logical.detail());
        }
    }
}
