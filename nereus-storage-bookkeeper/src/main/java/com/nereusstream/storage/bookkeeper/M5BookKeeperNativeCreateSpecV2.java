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

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Exact native create scope; source/protocol publication and deletion authority remain separate. */
public record M5BookKeeperNativeCreateSpecV2(
        String nativeInstanceId, Sha256Digest taskId, List<RunLedgerConfigurationV1> configurations) {
    public static final int MAX_BYTES = 32_768;
    private static final int MAGIC = 0x4d354e43; // M5NC

    public M5BookKeeperNativeCreateSpecV2 {
        Objects.requireNonNull(nativeInstanceId, "nativeInstanceId");
        Objects.requireNonNull(taskId, "taskId");
        configurations = List.copyOf(configurations);
        if (!UUID.fromString(nativeInstanceId).toString().equals(nativeInstanceId)
                || taskId.isZero()
                || configurations.isEmpty()
                || configurations.size() > 256) {
            throw new IllegalArgumentException("native BK create scope identity or count differs");
        }
        var ids = new HashSet<StorageRunId>();
        var sorted = new ArrayList<>(configurations);
        sorted.sort(Comparator.comparing(value -> value.runId().value().toHex()));
        if (!sorted.equals(configurations)) {
            throw new IllegalArgumentException("native BK create configurations are not canonically ordered");
        }
        for (var configuration : configurations) {
            if (!ids.add(configuration.runId())
                    || configuration.digestType() != BookKeeperDigestTypeV1.CRC32C
                    || !configuration
                            .providerScopeId()
                            .equals(configurations.get(0).providerScopeId())) {
                throw new IllegalArgumentException("native BK create configurations differ or repeat a run");
            }
        }
    }

    public static M5BookKeeperNativeCreateSpecV2 of(
            String nativeInstanceId, Sha256Digest taskId, List<RunLedgerConfigurationV1> configurations) {
        var sorted = new ArrayList<>(configurations);
        sorted.sort(Comparator.comparing(value -> value.runId().value().toHex()));
        return new M5BookKeeperNativeCreateSpecV2(nativeInstanceId, taskId, sorted);
    }

    public Namespace namespace() {
        return namespace(nativeInstanceId);
    }

    public static Namespace namespace(String nativeInstanceId) {
        if (!UUID.fromString(nativeInstanceId).toString().equals(nativeInstanceId)) {
            throw new IllegalArgumentException("native BookKeeper INSTANCEID is not canonical");
        }
        return new Namespace(
                ProviderKind.BOOKKEEPER,
                CanonicalUtf8.fromString("bookkeeper-instance:" + nativeInstanceId),
                CanonicalUtf8.fromString("native-ledger-id-space-v1"));
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeByte(2);
            out.writeUTF(nativeInstanceId);
            out.write(taskId.bytes().toByteArray());
            out.writeShort(configurations.size());
            for (var configuration : configurations) {
                out.write(configuration.providerScopeId().digest().bytes().toByteArray());
                out.write(configuration.runId().value().bytes().toByteArray());
                out.writeInt(configuration.ensembleSize());
                out.writeInt(configuration.writeQuorumSize());
                out.writeInt(configuration.ackQuorumSize());
                out.write(configuration.configurationDigest().bytes().toByteArray());
            }
            out.flush();
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("native BK create scope exceeds its byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public Sha256Digest sha256() {
        return Sha256Digest.hash(encode());
    }

    public static M5BookKeeperNativeCreateSpecV2 decode(CanonicalBytes bytes) {
        if (bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("native BK create scope byte length differs");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
            if (in.readInt() != MAGIC || in.readUnsignedByte() != 2) {
                throw new IllegalArgumentException("native BK create scope magic/version differs");
            }
            String instance = in.readUTF();
            var task = Sha256Digest.copyOf(in.readNBytes(32));
            int count = in.readUnsignedShort();
            if (count < 1 || count > 256) {
                throw new IllegalArgumentException("native BK create scope count exceeds its cap");
            }
            var configurations = new ArrayList<RunLedgerConfigurationV1>();
            for (int i = 0; i < count; i++) {
                configurations.add(new RunLedgerConfigurationV1(
                        new CellProviderScopeId(Sha256Digest.copyOf(in.readNBytes(32))),
                        new StorageRunId(Id128.fromBytes(in.readNBytes(16))),
                        in.readInt(),
                        in.readInt(),
                        in.readInt(),
                        BookKeeperDigestTypeV1.CRC32C,
                        Sha256Digest.copyOf(in.readNBytes(32))));
            }
            var spec = new M5BookKeeperNativeCreateSpecV2(instance, task, configurations);
            if (in.read() != -1 || !spec.encode().equals(bytes)) {
                throw new IllegalArgumentException("native BK create scope is not canonical");
            }
            return spec;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("native BK create scope is truncated", malformed);
        }
    }
}
