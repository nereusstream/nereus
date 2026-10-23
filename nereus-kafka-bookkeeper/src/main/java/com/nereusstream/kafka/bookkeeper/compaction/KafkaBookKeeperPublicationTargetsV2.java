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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Immutable original publication membership for terminal cleanup; it grants no new work or deletion authority. */
record KafkaBookKeeperPublicationTargetsV2(
        Sha256Digest taskId,
        Sha256Digest descriptorId,
        Sha256Digest sourceCutSha256,
        List<PhysicalResourceIdV2> resources) {
    private static final int MAGIC = 0x4b425054; // KBPT
    private static final int VERSION = 1;
    private static final int MAX_BYTES = 1_048_576;

    KafkaBookKeeperPublicationTargetsV2 {
        KafkaCompactionRecordsV1.requireDigest(taskId, "taskId");
        KafkaCompactionRecordsV1.requireDigest(descriptorId, "descriptorId");
        KafkaCompactionRecordsV1.requireDigest(sourceCutSha256, "sourceCutSha256");
        resources = List.copyOf(resources);
        if (!resources.equals(M5TargetDeleteMultiWriterGuardV2.canonicalTargets(resources))) {
            throw new IllegalArgumentException("publication target set is not sorted and unique");
        }
    }

    static KafkaBookKeeperPublicationTargetsV2 of(
            KafkaSealedBookKeeperDescriptorV2 descriptor, List<PhysicalResourceIdV2> resources) {
        return new KafkaBookKeeperPublicationTargetsV2(
                descriptor.task().taskIdSha256(),
                descriptor.descriptorSha256(),
                M5MaterializationCodecV1.sourceCutSha256(descriptor.sourceCut()),
                resources);
    }

    String key() {
        return key(descriptorId);
    }

    static String key(Sha256Digest descriptorId) {
        return "v2/kafka-bk-compaction-v2/views/" + descriptorId.toHex() + "/publication-targets";
    }

    void requireDescriptor(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        if (!equals(of(descriptor, resources))) {
            throw new IllegalArgumentException("original publication targets belong to another task or source cut");
        }
    }

    CanonicalBytes encode() {
        int length = 12 + 4 * Sha256Digest.LENGTH;
        for (var resource : resources) {
            length = Math.addExact(
                    length, Integer.BYTES + resource.canonicalBytes().length());
        }
        if (length > MAX_BYTES) {
            throw new IllegalArgumentException("publication target value exceeds the native metadata bound");
        }
        var out = ByteBuffer.allocate(length)
                .putInt(MAGIC)
                .putInt(VERSION)
                .put(taskId.bytes().toByteArray())
                .put(descriptorId.bytes().toByteArray())
                .put(sourceCutSha256.bytes().toByteArray())
                .putInt(resources.size());
        for (var resource : resources) {
            var bytes = resource.canonicalBytes();
            out.putInt(bytes.length()).put(bytes.toByteArray());
        }
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(java.util.Arrays.copyOf(out.array(), out.position())))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    static KafkaBookKeeperPublicationTargetsV2 decode(CanonicalBytes bytes) {
        if (bytes.length() < 12 + 4 * Sha256Digest.LENGTH || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("publication target value length is invalid");
        }
        try {
            var in = ByteBuffer.wrap(bytes.toByteArray());
            if (in.getInt() != MAGIC || in.getInt() != VERSION) {
                throw new IllegalArgumentException("publication target wire is unknown");
            }
            var task = digest(in);
            var descriptor = digest(in);
            var cut = digest(in);
            int count = in.getInt();
            if (count <= 0 || count > M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS) {
                throw new IllegalArgumentException("publication target count is invalid");
            }
            var resources = new ArrayList<PhysicalResourceIdV2>();
            for (int ordinal = 0; ordinal < count; ordinal++) {
                int length = in.getInt();
                if (length <= 0
                        || length > PhysicalResourceIdV2.MAX_ENCODED_BYTES
                        || length > in.remaining() - Sha256Digest.LENGTH) {
                    throw new IllegalArgumentException("publication target resource length is invalid");
                }
                byte[] resource = new byte[length];
                in.get(resource);
                resources.add(PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(resource)));
            }
            var value = new KafkaBookKeeperPublicationTargetsV2(task, descriptor, cut, resources);
            if (!value.encode().equals(bytes)) {
                throw new IllegalArgumentException("publication target value is noncanonical or corrupt");
            }
            return value;
        } catch (java.nio.BufferUnderflowException malformed) {
            throw new IllegalArgumentException("truncated publication target value", malformed);
        }
    }

    private static Sha256Digest digest(ByteBuffer input) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        input.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }
}
