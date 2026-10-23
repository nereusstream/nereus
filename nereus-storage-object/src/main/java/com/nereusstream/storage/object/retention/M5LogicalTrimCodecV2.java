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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimFenceV2;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimMutationV2;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimStateV2;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Optional;

/** Bounded nested fields; the enclosing authority supplies canonical hashing and version dispatch. */
final class M5LogicalTrimCodecV2 {
    private M5LogicalTrimCodecV2() {}

    static void writeState(DataOutputStream out, LogicalTrimStateV2 state) throws IOException {
        out.writeBoolean(state.frontier().isPresent());
        if (state.frontier().isPresent()) {
            writeBytes(
                    out, M5RetentionCodecV1.encodeTrimFrontier(state.frontier().orElseThrow()), 1_048_576);
        }
        out.writeBoolean(state.fence().isPresent());
        if (state.fence().isPresent()) {
            var fence = state.fence().orElseThrow();
            writeDigest(out, fence.attemptIdSha256());
            writeDigest(out, fence.floorSnapshotRootSha256());
            var fact = fence.openAuthority();
            writeBytes(out, CanonicalUtf8.fromString(fact.key()).bytes(), 1024);
            writeBytes(out, fact.metadataVersion().value(), 1024);
            writeDigest(out, fact.valueSha256());
        }
    }

    static LogicalTrimStateV2 readState(DataInputStream in) throws IOException {
        var frontier = in.readBoolean()
                ? Optional.of(M5RetentionCodecV1.decodeTrimFrontier(readBytes(in, 1_048_576)))
                : Optional.<M5RetentionRecordsV1.BindingTrimFrontierV1>empty();
        Optional<LogicalTrimFenceV2> fence = Optional.empty();
        if (in.readBoolean()) {
            var attempt = readDigest(in);
            var root = readDigest(in);
            var key = CanonicalUtf8.fromBytes(readBytes(in, 1024).toByteArray()).value();
            var version = new MetadataVersion(readBytes(in, 1024));
            fence = Optional.of(
                    new LogicalTrimFenceV2(attempt, root, new AuthorityFactV1(key, version, readDigest(in))));
        }
        return new LogicalTrimStateV2(frontier, fence);
    }

    static void writeMutation(DataOutputStream out, LogicalTrimMutationV2 mutation) throws IOException {
        writeDigest(out, mutation.operationId());
        writeBytes(out, CanonicalUtf8.fromString(mutation.key()).bytes(), 1024);
        out.writeBoolean(mutation.predecessorVersion().isPresent());
        if (mutation.predecessorVersion().isPresent()) {
            writeBytes(out, mutation.predecessorVersion().orElseThrow().value(), 4096);
            writeDigest(out, mutation.predecessorSha256().orElseThrow());
        }
        writeDigest(out, mutation.candidateSha256());
    }

    static LogicalTrimMutationV2 readMutation(DataInputStream in) throws IOException {
        var operation = readDigest(in);
        var key = CanonicalUtf8.fromBytes(readBytes(in, 1024).toByteArray()).value();
        Optional<MetadataVersion> version = Optional.empty();
        Optional<Sha256Digest> predecessor = Optional.empty();
        if (in.readBoolean()) {
            version = Optional.of(new MetadataVersion(readBytes(in, 4096)));
            predecessor = Optional.of(readDigest(in));
        }
        return new LogicalTrimMutationV2(operation, key, version, predecessor, readDigest(in));
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest digest) throws IOException {
        out.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream in) throws IOException {
        var bytes = in.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated trim digest");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes bytes, int maximum) throws IOException {
        if (bytes.isEmpty() || bytes.length() > maximum) {
            throw new IllegalArgumentException("nested trim value exceeds its bound");
        }
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream in, int maximum) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > maximum) {
            throw new IllegalArgumentException("nested trim value length exceeds its bound");
        }
        var bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated trim value");
        }
        return CanonicalBytes.copyOf(bytes);
    }
}
