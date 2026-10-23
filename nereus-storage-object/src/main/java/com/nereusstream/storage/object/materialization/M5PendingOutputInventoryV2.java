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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Chosen keys before any Provider create. No invented version/resource ID or physical capacity grant is stored. */
public record M5PendingOutputInventoryV2(
        M5MaterializationWorkerV1 worker, M5CellProviderProfileV2 profile, State state, List<Slot> outputs) {
    private static final int MAGIC = 0x4d35504f; // M5PO
    public static final int MAX_BYTES = 1_048_576;
    public static final int MAX_OBJECT_KEY_BYTES = 1024;
    public static final int MAX_OUTPUTS = M5MaterializationRecordsV1.MAX_PARTS + M5MaterializationRecordsV1.MAX_INDEXES;

    public enum State {
        PREPARED,
        CANCELLED_UNSTARTED
    }

    public enum Role {
        PAYLOAD,
        INDEX
    }

    public record Slot(Role role, int ordinal, String objectKey) {
        public Slot {
            Objects.requireNonNull(role, "role");
            if (ordinal < 0
                    || ordinal
                            >= (role == Role.PAYLOAD
                                    ? M5MaterializationRecordsV1.MAX_PARTS
                                    : M5MaterializationRecordsV1.MAX_INDEXES)
                    || objectKey == null
                    || objectKey.isEmpty()
                    || objectKey.indexOf('\0') >= 0
                    || CanonicalUtf8.fromString(objectKey).bytes().length() > MAX_OBJECT_KEY_BYTES) {
                throw new IllegalArgumentException("pending output key/ordinal exceeds its bound");
            }
        }
    }

    public M5PendingOutputInventoryV2 {
        Objects.requireNonNull(worker, "worker");
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(state, "state");
        outputs = List.copyOf(outputs);
        if (!worker.identity().providerScopeSha256().equals(profile.scopeId().digest())
                || !worker.identity().protocolCellSha256().equals(profile.protocolCellSha256())
                || outputs.size() > MAX_OUTPUTS
                || !outputs.equals(outputs.stream()
                        .sorted(Comparator.comparing(Slot::role).thenComparingInt(Slot::ordinal))
                        .toList())
                || outputs.stream().map(Slot::objectKey).distinct().count() != outputs.size()
                || outputs.stream()
                                .map(slot -> slot.role().name() + ":" + slot.ordinal())
                                .distinct()
                                .count()
                        != outputs.size()
                || outputs.stream().anyMatch(slot -> !slot.objectKey()
                        .startsWith(profile.exclusiveNamespacePrefix() + "/m5/materialization/"
                                + worker.taskIdSha256().toHex() + "/"))) {
            throw new IllegalArgumentException("pending outputs differ from the original Cell/scope or key namespace");
        }
    }

    public static M5PendingOutputInventoryV2 prepare(
            MaterializationPlan plan, M5CellProviderProfileV2 profile, M5MaterializationWorkerV1 worker) {
        var inventory = new M5PendingOutputInventoryV2(worker, profile, State.PREPARED, plannedSlots(plan));
        inventory.requirePlan(plan);
        return inventory;
    }

    public void requirePlan(MaterializationPlan plan) {
        if (!worker.identity().equals(plan.sourceCut().identity())
                || !worker.taskIdSha256().equals(plan.taskIdSha256())
                || !worker.sourceCutSha256().equals(M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()))
                || !worker.planRootSha256().equals(Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan)))
                || !outputs.equals(plannedSlots(plan))) {
            throw new IllegalArgumentException("pending inventory lacks its exact complete original plan/worker");
        }
    }

    /** Caller must prove this original actor never dispatched create. It grants no delete/absence authority. */
    public M5PendingOutputInventoryV2 cancelUnstarted() {
        return new M5PendingOutputInventoryV2(worker, profile, State.CANCELLED_UNSTARTED, outputs);
    }

    private static List<Slot> plannedSlots(MaterializationPlan plan) {
        var slots = new ArrayList<Slot>();
        if (plan.representationMode() == RepresentationMode.REWRITE_GENERATION) {
            for (var part : plan.outputParts()) {
                slots.add(new Slot(Role.PAYLOAD, part.ordinal(), part.objectKey()));
            }
        }
        for (int ordinal = 0; ordinal < plan.indexes().size(); ordinal++) {
            slots.add(new Slot(Role.INDEX, ordinal, plan.indexes().get(ordinal).objectKey()));
        }
        return List.copyOf(slots);
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(2);
                blob(out, worker.encode());
                blob(out, profile.encode());
                out.writeByte(state.ordinal());
                out.writeInt(outputs.size());
                for (var slot : outputs) {
                    out.writeByte(slot.role().ordinal());
                    out.writeInt(slot.ordinal());
                    blob(out, CanonicalUtf8.fromString(slot.objectKey()).bytes());
                }
            }
            var encoded = CanonicalBytes.copyOf(bytes.toByteArray());
            if (encoded.length() > MAX_BYTES) {
                throw new IllegalArgumentException("pending inventory exceeds canonical byte bound");
            }
            return encoded;
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode pending output inventory", impossible);
        }
    }

    public static M5PendingOutputInventoryV2 decode(CanonicalBytes bytes) {
        if (bytes == null || bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("pending inventory bytes exceed their bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 2) {
                throw new IllegalArgumentException("pending inventory magic/version differs");
            }
            var worker = M5MaterializationWorkerV1.decode(blob(in, M5MaterializationWorkerV1.BYTES));
            var profile = M5CellProviderProfileV2.decode(blob(in, M5CellProviderProfileV2.MAX_BYTES));
            int state = in.readUnsignedByte();
            int count = in.readInt();
            if (state >= State.values().length || count < 0 || count > MAX_OUTPUTS) {
                throw new IllegalArgumentException("pending inventory state/count exceeds its bound");
            }
            var slots = new ArrayList<Slot>(count);
            for (int i = 0; i < count; i++) {
                int role = in.readUnsignedByte();
                if (role >= Role.values().length) {
                    throw new IllegalArgumentException("pending inventory role is unsupported");
                }
                slots.add(new Slot(
                        Role.values()[role],
                        in.readInt(),
                        new String(blob(in, MAX_OBJECT_KEY_BYTES).toByteArray(), StandardCharsets.UTF_8)));
            }
            var inventory = new M5PendingOutputInventoryV2(worker, profile, State.values()[state], slots);
            if (in.available() != 0 || !inventory.encode().equals(bytes)) {
                throw new IllegalArgumentException("pending inventory canonical bytes differ");
            }
            return inventory;
        } catch (IOException failure) {
            throw new IllegalArgumentException("truncated pending output inventory", failure);
        }
    }

    private static void blob(DataOutputStream out, CanonicalBytes value) throws IOException {
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes blob(DataInputStream in, int maximum) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maximum || length > in.available()) {
            throw new IllegalArgumentException("pending inventory field exceeds its byte bound");
        }
        return CanonicalBytes.copyOf(in.readNBytes(length));
    }
}
