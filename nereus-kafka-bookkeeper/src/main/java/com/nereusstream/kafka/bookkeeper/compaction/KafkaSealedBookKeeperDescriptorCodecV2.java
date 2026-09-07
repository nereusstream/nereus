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
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.SemanticValidationProof;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Strict bounded KBSD2 descriptor codec; task and part identities use their existing independent domains. */
public final class KafkaSealedBookKeeperDescriptorCodecV2 {
    private static final int MAGIC = 0x4b425344;
    private static final byte[] KIND = KafkaSealedBookKeeperDescriptorV2.KIND.getBytes(StandardCharsets.US_ASCII);

    private KafkaSealedBookKeeperDescriptorCodecV2() {}

    public static CanonicalBytes encode(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeShort(2);
                output.write(KIND);
                CanonicalBytes task = KafkaBookKeeperInventoryCodecV2.encodeTask(descriptor.task());
                output.writeInt(task.length());
                output.write(task.toByteArray());
                output.writeLong(descriptor.sourceGeneration());
                output.writeInt(descriptor.batchCount());
                digest(output, descriptor.dispositionRootSha256());
                digest(output, descriptor.gapRootSha256());
                var proof = descriptor.semanticProof();
                for (Sha256Digest value : List.of(
                        proof.taskIdSha256(),
                        proof.outputIdentitySha256(),
                        proof.sourceSetSha256(),
                        proof.semanticValidationRootSha256(),
                        proof.protocolStateRootSha256(),
                        proof.compactionSuppressionRootSha256(),
                        proof.payloadBodiesRootSha256(),
                        proof.indexBodiesRootSha256())) {
                    digest(output, value);
                }
                output.writeInt(descriptor.sealedParts().size());
                for (var seal : descriptor.sealedParts()) {
                    output.writeLong(seal.handle().ledgerIdentity().ledgerId());
                    output.writeLong(seal.sealedLastEntryId());
                    output.writeLong(seal.sealedLength());
                    output.writeInt(seal.metadataFormatVersion());
                    output.writeLong(seal.metadataCToken());
                    digest(output, seal.passwordSha256());
                    digest(output, seal.metadataSha256());
                }
                output.writeInt(descriptor.indexes().size());
                for (var locator : descriptor.indexes()) {
                    output.writeInt(locator.firstPart());
                    output.writeInt(locator.firstEntry());
                    output.writeInt(locator.chunkCount());
                    output.writeInt(locator.artifactLength());
                    digest(output, locator.artifactSha256());
                }
            }
            if (bytes.size() > KafkaSealedBookKeeperDescriptorV2.MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("sealed BK descriptor exceeds its encoded bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory sealed BK descriptor encoding failed", impossible);
        }
    }

    public static KafkaSealedBookKeeperDescriptorV2 decode(CanonicalBytes bytes) {
        if (bytes.length() > KafkaSealedBookKeeperDescriptorV2.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("sealed BK descriptor exceeds its encoded bound");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (input.readInt() != MAGIC
                    || input.readUnsignedShort() != 2
                    || !java.util.Arrays.equals(input.readNBytes(KIND.length), KIND)) {
                throw new IllegalArgumentException("sealed BK descriptor preamble or kind differs");
            }
            int length = input.readInt();
            if (length <= 0 || length > KafkaBookKeeperInventoryV2.MAX_TASK_BYTES || length > input.available()) {
                throw new IllegalArgumentException("sealed BK descriptor task length exceeds bound");
            }
            var task = KafkaBookKeeperInventoryCodecV2.decodeTask(CanonicalBytes.copyOf(input.readNBytes(length)));
            long generation = input.readLong();
            int batches = input.readInt();
            Sha256Digest disposition = digest(input);
            Sha256Digest gaps = digest(input);
            var proof = new SemanticValidationProof(
                    digest(input),
                    digest(input),
                    digest(input),
                    digest(input),
                    digest(input),
                    digest(input),
                    digest(input),
                    digest(input));
            int count = input.readInt();
            if (count != task.parts().size()) {
                throw new IllegalArgumentException("sealed BK descriptor part count differs from its task");
            }
            List<BookKeeperDeleteTargetV1> seals = new ArrayList<>(count);
            for (int ordinal = 0; ordinal < count; ordinal++) {
                var configuration = task.configuration(ordinal);
                var capability = task.capability();
                var handle = new RunLedgerHandleV1(
                        configuration.providerScopeId(),
                        configuration.runId(),
                        new BookKeeperLedgerIdentity(input.readLong()),
                        configuration.configurationDigest());
                long lac = input.readLong();
                long sealedLength = input.readLong();
                int metadataVersion = input.readInt();
                long token = input.readLong();
                seals.add(new BookKeeperDeleteTargetV1(
                        handle,
                        lac,
                        sealedLength,
                        capability.ensembleSize(),
                        capability.writeQuorumSize(),
                        capability.ackQuorumSize(),
                        capability.digestType(),
                        capability.credentialIdentityVersion(),
                        digest(input),
                        metadataVersion,
                        token,
                        digest(input)));
            }
            if (input.readInt() != 8) {
                throw new IllegalArgumentException("sealed BK descriptor lacks exactly eight indexes");
            }
            List<KafkaSealedBookKeeperDescriptorV2.IndexLocator> indexes = new ArrayList<>(8);
            for (int ordinal = 0; ordinal < 8; ordinal++) {
                indexes.add(new KafkaSealedBookKeeperDescriptorV2.IndexLocator(
                        input.readInt(), input.readInt(), input.readInt(), input.readInt(), digest(input)));
            }
            var descriptor = new KafkaSealedBookKeeperDescriptorV2(
                    task, generation, batches, disposition, gaps, proof, seals, indexes);
            if (input.available() != 0 || !encode(descriptor).equals(bytes)) {
                throw new IllegalArgumentException("sealed BK descriptor is noncanonical or has trailing bytes");
            }
            return descriptor;
        } catch (IOException failure) {
            throw new IllegalArgumentException("truncated sealed BK descriptor", failure);
        }
    }

    private static void digest(DataOutputStream output, Sha256Digest value) throws IOException {
        KafkaBookKeeperInventoryV2.requireDigest(value);
        output.write(value.bytes().toByteArray());
    }

    private static Sha256Digest digest(DataInputStream input) throws IOException {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        input.readFully(bytes);
        Sha256Digest value = Sha256Digest.copyOf(bytes);
        KafkaBookKeeperInventoryV2.requireDigest(value);
        return value;
    }
}
