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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Fixed Native data/cursor create scope. Its durable native fence is separate from deletion eligibility. */
public record M5PulsarNativeCreateSpecV1(
        String nativeInstanceId,
        PhysicalNamespaceAuthorityBindingV2 namespaceBinding,
        CanonicalBytes protocolCell,
        CanonicalBytes allocatorOwner,
        Sha256Digest providerScope,
        Sha256Digest configuration,
        int ensemble,
        int writeQuorum,
        int ackQuorum,
        List<Binding> bindings,
        Optional<java.util.UUID> factoryInstance,
        Optional<BookKeeperGcGraceV1> bookKeeperGcGrace) {
    public static final int MAX_BYTES = 1_048_576;
    public static final int MAX_METADATA_BYTES = 1_048_576;
    public static final int MAX_PENDING_METADATA_MUTATIONS = 2;
    public static final int MAX_PENDING_LEDGER_ID_GENERATIONS = 2;
    public static final int MAX_BIRTH_READS = 1;
    public static final int MAX_SDK_METADATA_HANDLES = 1024;
    public static final int SDK_METADATA_HANDLE_BYTES = 4096;
    public static final int MAX_PENDING_SDK_DATA_RPCS = 256;
    public static final long MAX_PENDING_SDK_DATA_BYTES = 32L * 1024 * 1024;
    public static final int SDK_DATA_RPC_DESCRIPTOR_BYTES = 512;
    public static final int MAX_BULK_METADATA_BYTES = 2 * MAX_METADATA_BYTES;
    // Two exact index cuts (up to 1,024 entries and 2 KiB roots), scope bytes and bounded raw headers.
    public static final int BIRTH_READ_WORKSPACE_BYTES = 16 * MAX_METADATA_BYTES;
    static final String SCOPE_KEY = "nereus/m5-pulsar-create-scope";
    static final String BINDING_KEY = "nereus/m5-pulsar-binding";

    /** Explicit confirmed-drain profile only. Absence preserves historical scopes without granting BK GC. */
    public record BookKeeperGcGraceV1(long providerMinimumMillis, long auditMinimumMillis, long durationMillis) {
        public BookKeeperGcGraceV1 {
            if (providerMinimumMillis < 0
                    || auditMinimumMillis <= 0
                    || durationMillis <= 0
                    || durationMillis > Long.MAX_VALUE / 1_000_000
                    || durationMillis < providerMinimumMillis
                    || durationMillis < auditMinimumMillis) {
                throw new IllegalArgumentException("Native BK GC grace does not meet explicit positive minima");
            }
        }
    }

    public M5PulsarNativeCreateSpecV1(
            String nativeInstanceId,
            PhysicalNamespaceAuthorityBindingV2 namespaceBinding,
            CanonicalBytes protocolCell,
            CanonicalBytes allocatorOwner,
            Sha256Digest providerScope,
            Sha256Digest configuration,
            int ensemble,
            int writeQuorum,
            int ackQuorum,
            List<Binding> bindings,
            Optional<java.util.UUID> factoryInstance) {
        this(
                nativeInstanceId,
                namespaceBinding,
                protocolCell,
                allocatorOwner,
                providerScope,
                configuration,
                ensemble,
                writeQuorum,
                ackQuorum,
                bindings,
                factoryInstance,
                Optional.empty());
    }

    /** Existing v1 descriptor bytes remain unchanged; a new factory explicitly allocates its own v2 scope. */
    public M5PulsarNativeCreateSpecV1(
            String nativeInstanceId,
            PhysicalNamespaceAuthorityBindingV2 namespaceBinding,
            CanonicalBytes protocolCell,
            CanonicalBytes allocatorOwner,
            Sha256Digest providerScope,
            Sha256Digest configuration,
            int ensemble,
            int writeQuorum,
            int ackQuorum,
            List<Binding> bindings) {
        this(
                nativeInstanceId,
                namespaceBinding,
                protocolCell,
                allocatorOwner,
                providerScope,
                configuration,
                ensemble,
                writeQuorum,
                ackQuorum,
                bindings,
                Optional.empty());
    }

    public M5PulsarNativeCreateSpecV1 forFactory(java.util.UUID instance) {
        return new M5PulsarNativeCreateSpecV1(
                nativeInstanceId,
                namespaceBinding,
                protocolCell,
                allocatorOwner,
                providerScope,
                configuration,
                ensemble,
                writeQuorum,
                ackQuorum,
                bindings,
                Optional.of(instance),
                bookKeeperGcGrace);
    }

    /** First explicit admission; existing scopes and ledger tags are never rewritten by this derivation. */
    public M5PulsarNativeCreateSpecV1 forFactory(java.util.UUID instance, BookKeeperGcGraceV1 grace) {
        return new M5PulsarNativeCreateSpecV1(
                nativeInstanceId,
                namespaceBinding,
                protocolCell,
                allocatorOwner,
                providerScope,
                configuration,
                ensemble,
                writeQuorum,
                ackQuorum,
                bindings,
                Optional.of(instance),
                Optional.of(grace));
    }

    public record Binding(String managedLedgerName, CanonicalBytes identity) {
        public Binding {
            Objects.requireNonNull(managedLedgerName, "managed ledger name");
            Objects.requireNonNull(identity, "Binding identity");
            if (managedLedgerName.isEmpty()
                    || managedLedgerName.indexOf('\0') >= 0
                    || managedLedgerName.getBytes(StandardCharsets.UTF_8).length > 2048
                    || !new String(managedLedgerName.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8)
                            .equals(managedLedgerName)
                    || identity.length() != 96) {
                throw new IllegalArgumentException("Native Pulsar Binding name/identity is invalid");
            }
            for (int offset = 0; offset < 96; offset += 32) {
                if (Sha256Digest.copyOf(Arrays.copyOfRange(identity.toByteArray(), offset, offset + 32))
                        .isZero()) {
                    throw new IllegalArgumentException("Native Pulsar Binding digest is zero");
                }
            }
        }
    }

    public M5PulsarNativeCreateSpecV1 {
        Objects.requireNonNull(namespaceBinding, "namespace binding");
        Objects.requireNonNull(protocolCell, "protocol Cell");
        Objects.requireNonNull(allocatorOwner, "allocator owner");
        Objects.requireNonNull(providerScope, "provider scope");
        Objects.requireNonNull(configuration, "configuration");
        bindings = List.copyOf(bindings);
        factoryInstance = Objects.requireNonNull(factoryInstance, "factory instance");
        bookKeeperGcGrace = Objects.requireNonNull(bookKeeperGcGrace, "BookKeeper GC grace");
        if (bookKeeperGcGrace.isPresent() && factoryInstance.isEmpty()) {
            throw new IllegalArgumentException("Native BK GC requires an explicit factory scope");
        }
        factoryInstance.ifPresent(value -> {
            if (value.getMostSignificantBits() == 0 && value.getLeastSignificantBits() == 0) {
                throw new IllegalArgumentException("Native factory instance is zero");
            }
        });
        if (!namespaceBinding.physicalNamespace().equals(M5BookKeeperNativeCreateSpecV2.namespace(nativeInstanceId))
                || protocolCell.isEmpty()
                || protocolCell.length() > 4096
                || allocatorOwner.isEmpty()
                || allocatorOwner.length() > 4096
                || providerScope.isZero()
                || configuration.isZero()
                || ensemble < 1
                || writeQuorum < 1
                || ackQuorum < 1
                || ackQuorum > writeQuorum
                || writeQuorum > ensemble
                || bindings.isEmpty()
                || bindings.size() > 1024
                || !bindings.equals(bindings.stream()
                        .sorted(Comparator.comparing(Binding::managedLedgerName))
                        .toList())
                || bindings.stream().map(Binding::managedLedgerName).distinct().count() != bindings.size()) {
            throw new IllegalArgumentException("Native Pulsar create scope is invalid");
        }
    }

    public Sha256Digest sha256() {
        return Sha256Digest.hash(encode());
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.writeInt(0x4d355043);
            out.writeInt(bookKeeperGcGrace.isPresent() ? 3 : factoryInstance.isPresent() ? 2 : 1);
            if (factoryInstance.isPresent()) {
                out.writeLong(factoryInstance.orElseThrow().getMostSignificantBits());
                out.writeLong(factoryInstance.orElseThrow().getLeastSignificantBits());
            }
            out.writeUTF(nativeInstanceId);
            write(out, namespaceBinding.encode());
            write(out, protocolCell);
            write(out, allocatorOwner);
            out.write(providerScope.bytes().toByteArray());
            out.write(configuration.bytes().toByteArray());
            out.writeInt(ensemble);
            out.writeInt(writeQuorum);
            out.writeInt(ackQuorum);
            out.writeInt(bindings.size());
            for (var binding : bindings) {
                out.writeUTF(binding.managedLedgerName());
                out.write(binding.identity().toByteArray());
            }
            if (bookKeeperGcGrace.isPresent()) {
                var grace = bookKeeperGcGrace.orElseThrow();
                out.writeLong(grace.providerMinimumMillis());
                out.writeLong(grace.auditMinimumMillis());
                out.writeLong(grace.durationMillis());
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("Native Pulsar scope exceeds its byte limit");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (java.io.IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static M5PulsarNativeCreateSpecV1 decode(CanonicalBytes bytes) {
        if (bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Native Pulsar scope byte length is invalid");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
            if (in.readInt() != 0x4d355043) {
                throw new IllegalArgumentException("Native Pulsar scope format is invalid");
            }
            int format = in.readInt();
            if (format != 1 && format != 2 && format != 3) {
                throw new IllegalArgumentException("Native Pulsar scope version is invalid");
            }
            Optional<java.util.UUID> factory =
                    format >= 2 ? Optional.of(new java.util.UUID(in.readLong(), in.readLong())) : Optional.empty();
            String instance = in.readUTF();
            var namespace = PhysicalNamespaceAuthorityBindingV2.decode(read(in));
            var cell = read(in);
            var owner = read(in);
            var provider = Sha256Digest.copyOf(in.readNBytes(32));
            var configuration = Sha256Digest.copyOf(in.readNBytes(32));
            int ensemble = in.readInt();
            int write = in.readInt();
            int ack = in.readInt();
            int count = in.readInt();
            if (count < 1 || count > 1024) {
                throw new IllegalArgumentException("Native Pulsar Binding count is invalid");
            }
            var bindings = new ArrayList<Binding>();
            for (int index = 0; index < count; index++) {
                bindings.add(new Binding(in.readUTF(), CanonicalBytes.copyOf(in.readNBytes(96))));
            }
            var grace = format == 3
                    ? Optional.of(new BookKeeperGcGraceV1(in.readLong(), in.readLong(), in.readLong()))
                    : Optional.<BookKeeperGcGraceV1>empty();
            var spec = new M5PulsarNativeCreateSpecV1(
                    instance,
                    namespace,
                    cell,
                    owner,
                    provider,
                    configuration,
                    ensemble,
                    write,
                    ack,
                    bindings,
                    factory,
                    grace);
            if (in.available() != 0 || !spec.encode().equals(bytes)) {
                throw new IllegalArgumentException("Native Pulsar scope is not canonical");
            }
            return spec;
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Native Pulsar scope is truncated", failure);
        }
    }

    public Optional<Binding> bindingFor(LedgerMetadata metadata, boolean tagged) {
        var values = metadata.getCustomMetadata();
        var name = values.get("pulsar/managed-ledger");
        if (metadata.getMetadataFormatVersion() != 3
                || metadata.getDigestType() != DigestType.CRC32C
                || metadata.getPassword().length != 0
                || metadata.getEnsembleSize() != ensemble
                || metadata.getWriteQuorumSize() != writeQuorum
                || metadata.getAckQuorumSize() != ackQuorum
                || !Arrays.equals(values.get("application"), "pulsar".getBytes(StandardCharsets.UTF_8))
                || !Arrays.equals(values.get("component"), "managed-ledger".getBytes(StandardCharsets.UTF_8))
                || name == null
                || values.size() != 3 + (values.containsKey("pulsar/cursor") ? 1 : 0) + (tagged ? 2 : 0)
                || values.containsKey("pulsar/cursor") && values.get("pulsar/cursor").length == 0) {
            return Optional.empty();
        }
        return bindings.stream()
                .filter(binding ->
                        Arrays.equals(name, binding.managedLedgerName().getBytes(StandardCharsets.UTF_8)))
                .filter(binding -> !tagged
                        || Arrays.equals(values.get(SCOPE_KEY), sha256().bytes().toByteArray())
                                && Arrays.equals(
                                        values.get(BINDING_KEY),
                                        binding.identity().toByteArray()))
                .findFirst();
    }

    /** Identify an actual sealed source using its stored quorum; never use this descriptor for SDK mutations. */
    public Optional<Binding> sealedSourceBindingFor(LedgerMetadata metadata) {
        if (!metadata.isClosed()
                || metadata.getAckQuorumSize() < 1
                || metadata.getWriteQuorumSize() < metadata.getAckQuorumSize()
                || metadata.getEnsembleSize() < metadata.getWriteQuorumSize()) {
            return Optional.empty();
        }
        var source = new M5PulsarNativeCreateSpecV1(
                nativeInstanceId,
                namespaceBinding,
                protocolCell,
                allocatorOwner,
                providerScope,
                configuration,
                metadata.getEnsembleSize(),
                metadata.getWriteQuorumSize(),
                metadata.getAckQuorumSize(),
                bindings,
                factoryInstance,
                bookKeeperGcGrace);
        var legacy = source.bindingFor(metadata, false);
        return legacy.isPresent() ? legacy : bindingFor(metadata, true);
    }

    /** Format-3 custom rows have no canonical order; compare every SDK field after parsing the original bytes. */
    static boolean sameMetadata(LedgerMetadata left, LedgerMetadata right) {
        return left.getLedgerId() == right.getLedgerId()
                && left.getMetadataFormatVersion() == right.getMetadataFormatVersion()
                && left.getState() == right.getState()
                && left.getEnsembleSize() == right.getEnsembleSize()
                && left.getWriteQuorumSize() == right.getWriteQuorumSize()
                && left.getAckQuorumSize() == right.getAckQuorumSize()
                && left.getLength() == right.getLength()
                && left.getLastEntryId() == right.getLastEntryId()
                && left.getCToken() == right.getCToken()
                && left.getCtime() == right.getCtime()
                && org.apache.bookkeeper.client.LedgerMetadataUtils.shouldStoreCtime(left)
                        == org.apache.bookkeeper.client.LedgerMetadataUtils.shouldStoreCtime(right)
                && left.hasPassword() == right.hasPassword()
                && left.getDigestType() == right.getDigestType()
                && Arrays.equals(left.getPassword(), right.getPassword())
                && left.getAllEnsembles().equals(right.getAllEnsembles())
                && left.getCustomMetadata()
                        .keySet()
                        .equals(right.getCustomMetadata().keySet())
                && left.getCustomMetadata().entrySet().stream()
                        .allMatch(entry -> Arrays.equals(
                                entry.getValue(), right.getCustomMetadata().get(entry.getKey())));
    }

    /** Bounds the format-3 payload before the SDK serializer allocates its protobuf and byte copies. */
    public static void requireBoundedMetadata(LedgerMetadata metadata) {
        if (metadata.getMetadataFormatVersion() != 3) {
            throw new IllegalArgumentException("Native Pulsar metadata format differs");
        }
        // BK 4.18 format 3: header/delimiter and fixed scalar fields fit in 256 bytes. Each nested
        // custom/segment row needs at most 32 framing bytes; each bookie string at most 8. UTF-8
        // requires at most three bytes per UTF-16 code unit, without allocating an encoding here.
        long bytes = 256L + metadata.getPassword().length;
        for (var entry : metadata.getCustomMetadata().entrySet()) {
            bytes += 32L + 3L * entry.getKey().length() + entry.getValue().length;
            requireMetadataBytes(bytes);
        }
        for (var ensemble : metadata.getAllEnsembles().values()) {
            bytes += 32;
            requireMetadataBytes(bytes);
            for (var bookie : ensemble) {
                bytes += 8L + 3L * bookie.toString().length();
                requireMetadataBytes(bytes);
            }
        }
        requireMetadataBytes(bytes);
    }

    private static void requireMetadataBytes(long bytes) {
        if (bytes > MAX_METADATA_BYTES) {
            throw new IllegalArgumentException("Native Pulsar metadata exceeds its byte admission");
        }
    }

    private static void write(DataOutputStream out, CanonicalBytes value) throws java.io.IOException {
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes read(DataInputStream in) throws java.io.IOException {
        int length = in.readInt();
        if (length < 1 || length > in.available()) {
            throw new IllegalArgumentException("Native Pulsar scope member is truncated");
        }
        return CanonicalBytes.copyOf(in.readNBytes(length));
    }
}
