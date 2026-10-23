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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.capability.PulsarActiveTopicAuthority;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.materialization.M5PulsarRecoveryInputV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Exact P1/native-view references. These bytes are input commitments, never worker or recovery authority. */
public record OxiaPulsarMaterializationInputV2(
        MetadataNamespaceIdentityV2 metadataNamespace,
        String authorityRoot,
        String aggregateKey,
        long aggregateVersion,
        CanonicalBytes aggregateValue,
        String selectorKey,
        long selectorVersion,
        CanonicalBytes selectorValue,
        String sourceViewKey,
        long sourceViewVersion,
        Sha256Digest sourceViewValueSha256,
        M5PulsarCoverageV2 coverage,
        Sha256Digest recoveryInputSha256) {
    private static final int MAGIC = 0x4e504d49; // NPMI
    private static final int SOURCE_MAP_HEADER_BYTES = 44;
    private static final int MAX_SOURCE_MAP_BYTES = 1_048_576;
    public static final int MAX_KEY_BYTES = 512;
    public static final int MAX_BYTES = M5PulsarCoverageV2.MAX_BYTES
            + Nta1CodecV1.MAX_NTA1_BYTES
            + com.nereusstream.metadata.oxia.v2.codec.Nps1SelectorAuthorityCodec.MAX_BYTES
            + 4096;

    public OxiaPulsarMaterializationInputV2 {
        Objects.requireNonNull(metadataNamespace, "metadataNamespace");
        requireText(authorityRoot);
        requireText(aggregateKey);
        requireText(selectorKey);
        requireText(sourceViewKey);
        MetadataVersionMapper.fromOxia(aggregateVersion);
        MetadataVersionMapper.fromOxia(selectorVersion);
        MetadataVersionMapper.fromOxia(sourceViewVersion);
        requireDigest(sourceViewValueSha256);
        requireDigest(recoveryInputSha256);
        Objects.requireNonNull(coverage, "coverage");
        var aggregate = Nta1CodecV1.decode(aggregateValue);
        if (!(aggregate.binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity incarnation)) {
            throw new IllegalArgumentException("Native materialization input requires a Pulsar incarnation");
        }
        var keys = new OxiaV2AuthorityKeys(authorityRoot);
        if (!keys.aggregateKey(incarnation).equals(aggregateKey)
                || !keys.selectorKey(incarnation.persistenceName()).equals(selectorKey)) {
            throw new IllegalArgumentException("Native input P1 authority keys differ");
        }
        var codecs = OxiaV2CodecSet.productionP1();
        var authority = new PulsarActiveTopicAuthority(
                codecs.selector()
                        .decode(
                                selectorKey,
                                incarnation.persistenceName(),
                                selectorValue,
                                MetadataVersionMapper.fromOxia(selectorVersion)),
                codecs.aggregate()
                        .decode(
                                aggregateKey,
                                incarnation,
                                aggregateValue,
                                MetadataVersionMapper.fromOxia(aggregateVersion)));
        var binding = new BindingIdentity(
                aggregate.binding().bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(incarnation)),
                aggregate.initialEpoch().storageEpochId().digest());
        if (!coverage.binding().equals(binding)
                || authority.selector().value().generation().value()
                        != incarnation.bindingGeneration().value()
                || aggregate.initialEpoch().storageProfile() != StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT
                || aggregate.initialEpoch().epochOrdinal() != 0) {
            throw new IllegalArgumentException("Native input Binding/Storage Epoch/profile differs");
        }
    }

    public BindingIdentity binding() {
        return coverage.binding();
    }

    public Sha256Digest protocolCellSha256() {
        return Sha256Digest.hash(ProtocolCellIdentityCodecV1.encode(
                Nta1CodecV1.decode(aggregateValue).binding().cellIdentity()));
    }

    public long storageFence() {
        return Math.addExact(Nta1CodecV1.decode(aggregateValue).initialEpoch().epochOrdinal(), 1);
    }

    public String managedLedgerName() {
        return ((PulsarTopicIncarnationIdentity)
                        Nta1CodecV1.decode(aggregateValue).binding().incarnationIdentity())
                .persistenceName()
                .value()
                .value();
    }

    public void requireCurrentP1(VersionedValue aggregate, VersionedValue selector) {
        var projected = OxiaV2CodecSet.productionP1().aggregate().projectStoredBytes(aggregate.canonicalStoredBytes());
        var sourceVersion =
                com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(
                                aggregate.canonicalStoredBytes())
                        ? com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                        aggregate.canonicalStoredBytes())
                                .originalAggregateVersion()
                                .orElse(aggregate.metadataVersion())
                        : aggregate.metadataVersion();
        if (!aggregateKey.equals(aggregate.key())
                || !selectorKey.equals(selector.key())
                || aggregateVersion != MetadataVersionMapper.toOxia(sourceVersion)
                || selectorVersion != MetadataVersionMapper.toOxia(selector.metadataVersion())
                || !aggregateValue.equals(projected)
                || !selectorValue.equals(selector.canonicalStoredBytes())) {
            throw new IllegalArgumentException("Native input P1 key/version/value changed");
        }
    }

    /** Independently reconstruct the metadata snapshot and exact directory from the original immutable view. */
    public M5PulsarRecoveryInputV2 verifySourceView(VersionedValue value) {
        if (!sourceViewKey.equals(value.key())
                || sourceViewVersion != MetadataVersionMapper.toOxia(value.metadataVersion())
                || !sourceViewValueSha256.equals(value.canonicalStoredSha256())) {
            throw new IllegalArgumentException("Native input source-view key/version/value differs");
        }
        var encoded = value.canonicalStoredBytes();
        if (encoded.length() <= SOURCE_MAP_HEADER_BYTES || encoded.length() > MAX_SOURCE_MAP_BYTES) {
            throw new IllegalArgumentException("Native input source-map length differs");
        }
        try (var map = input(encoded)) {
            if (map.readInt() != 0x4e50534d
                    || map.readInt() != 1
                    || !digest(map).equals(coverage.chainAuthorityRootSha256())) {
                throw new IllegalArgumentException("Native input source-map type/view differs");
            }
            var actualView = blob(map, MAX_SOURCE_MAP_BYTES - SOURCE_MAP_HEADER_BYTES);
            end(map);
            if (!Sha256Digest.hash(actualView).equals(coverage.chainAuthorityRootSha256())) {
                throw new IllegalArgumentException("Native input original view digest differs");
            }
            var projection = projectNativeView(actualView, 0);
            try (var in = input(projection.nativeView())) {
                if (in.readInt() != 0x4e504256
                        || in.readInt() != 1
                        || !text(blob(in, 128)).equals("NEREUS_V2_NATIVE_PULSAR_BINDING_READ_VIEW_V1")
                        || !digest(in).equals(binding().bindingId().digest())
                        || !digest(in).equals(binding().incarnationSha256())
                        || !digest(in).equals(binding().storageEpochSha256())
                        || !digest(in).equals(Sha256Digest.hash(aggregateValue))
                        || !blob(in, 8)
                                .equals(MetadataVersionMapper.fromOxia(aggregateVersion)
                                        .value())
                        || !blob(in, 8)
                                .equals(MetadataVersionMapper.fromOxia(selectorVersion)
                                        .value())) {
                    throw new IllegalArgumentException("Native input original P1 view differs");
                }
                var objects = PhysicalNamespaceAuthorityBindingV2.decode(blob(in, 512));
                if (projection.resources().stream()
                        .anyMatch(resource -> !resource.namespace().equals(objects.physicalNamespace()))) {
                    throw new IllegalArgumentException("Native input mapped output namespace differs");
                }
                if (!objects.metadataNamespace().equals(metadataNamespace)) {
                    throw new IllegalArgumentException("Native input metadata namespace differs");
                }
                long metadataVersion = in.readLong();
                var recovery = new M5PulsarRecoveryInputV2(
                        metadataNamespace,
                        binding(),
                        managedLedgerName(),
                        "/managed-ledgers/" + managedLedgerName(),
                        metadataVersion,
                        blob(in, M5PulsarRecoveryInputV2.MAX_METADATA_BYTES));
                if (!recovery.sha256().equals(recoveryInputSha256) || in.readLong() < 0 || in.readLong() < -1) {
                    throw new IllegalArgumentException("Native input recovery commitment/frontier differs");
                }
                int count = in.readInt();
                if (count < 0 || count > coverage.chain().size()) {
                    throw new IllegalArgumentException("Native input directory count differs");
                }
                var observed = new java.util.HashSet<Long>();
                long previous = -1;
                for (int i = 0; i < count; i++) {
                    long id = in.readLong();
                    if (id <= previous) {
                        throw new IllegalArgumentException("Native input directory ledger/order differs");
                    }
                    previous = id;
                    var ledger = coverage.chain().stream()
                            .filter(row -> row.ledgerId() == id)
                            .findFirst()
                            .orElseThrow();
                    int kind = in.readUnsignedByte();
                    var source = blob(in, MAX_SOURCE_MAP_BYTES);
                    if ((kind != 1 && kind != 2) || !Sha256Digest.hash(source).equals(ledger.sourceIdentitySha256())) {
                        throw new IllegalArgumentException("Native input original directory source differs");
                    }
                    requireLedgerBoundary(source, kind, ledger);
                    observed.add(id);
                }
                for (var ledger : coverage.chain()) {
                    if (!observed.contains(ledger.ledgerId())) {
                        var root = Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(44)
                                .putInt(0x4e504d4c)
                                .put(coverage.chainAuthorityRootSha256().bytes().toByteArray())
                                .putLong(ledger.ledgerId())
                                .array()));
                        if (ledger.active()
                                || !ledger.sourceIdentitySha256().equals(root)
                                || !projection.fullyMapped(ledger)) {
                            throw new IllegalArgumentException("Native input directory count/mapped ledger differs");
                        }
                    }
                }
                end(in);
                return recovery;
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException("Native input referenced view is truncated", failure);
        }
    }

    private record NativeProjection(
            CanonicalBytes nativeView,
            java.util.List<M5PulsarCoverageV2.EntryRange> rewrites,
            java.util.List<com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion> resources) {
        private boolean fullyMapped(M5PulsarCoverageV2.ChainLedger ledger) {
            long end = 0;
            for (var range : rewrites.stream()
                    .filter(value -> value.ledgerId() == ledger.ledgerId())
                    .sorted(java.util.Comparator.comparingLong(M5PulsarCoverageV2.EntryRange::startEntryInclusive))
                    .toList()) {
                if (range.startEntryInclusive() > end) {
                    return false;
                }
                end = Math.max(end, range.endEntryExclusive());
            }
            return end == ledger.entryEndExclusive() && end > 0;
        }
    }

    /** Metadata projection only. Full map/SDK/position validation remains the Native actor's separate gate. */
    private NativeProjection projectNativeView(CanonicalBytes view, int depth) throws IOException {
        if (depth > 64 || view.length() < 8 || view.length() > MAX_SOURCE_MAP_BYTES) {
            throw new IllegalArgumentException("Native input mapped projection exceeds its depth/byte bounds");
        }
        try (var in = input(view)) {
            int magic = in.readInt();
            int version = in.readInt();
            if (magic == 0x4e504256 && version == 1) {
                return new NativeProjection(view, java.util.List.of(), java.util.List.of());
            }
            if (magic != 0x4e504d56 || version != 2) {
                throw new IllegalArgumentException("Native input original view type differs");
            }
            digest(in); // The complete recipe digest is already bound to the actual selected view.
            var nativeView = blob(in, MAX_SOURCE_MAP_BYTES);
            var encodedMap = blob(in, MAX_SOURCE_MAP_BYTES);
            end(in);
            try (var map = input(encodedMap)) {
                if (map.readInt() != 0x4e504d4d || map.readInt() != 2) {
                    throw new IllegalArgumentException("Native input materialized map type differs");
                }
                var plan = com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodePlan(
                        blob(map, MAX_SOURCE_MAP_BYTES));
                if (!plan.sourceCut().identity().binding().equals(binding())
                        || !plan.sourceCut().coverage().typedPulsar()
                        || plan.representationMode()
                                != com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                        .RepresentationMode.REWRITE_GENERATION) {
                    throw new IllegalArgumentException("Native input mapped plan Binding/coverage differs");
                }
                var predecessor = blob(map, MAX_SOURCE_MAP_BYTES);
                final NativeProjection old;
                try (var source = input(predecessor)) {
                    if (source.readInt() != 0x4e50534d
                            || source.readInt() != 1
                            || !digest(source).equals(plan.sourceCut().predecessorViewSha256())) {
                        throw new IllegalArgumentException("Native input mapped predecessor differs");
                    }
                    var bytes = blob(source, MAX_SOURCE_MAP_BYTES);
                    end(source);
                    if (!Sha256Digest.hash(bytes).equals(plan.sourceCut().predecessorViewSha256())) {
                        throw new IllegalArgumentException("Native input mapped predecessor digest differs");
                    }
                    old = projectNativeView(bytes, depth + 1);
                }
                int count = map.readInt();
                if (count != plan.outputParts().size() + plan.indexes().size()) {
                    throw new IllegalArgumentException("Native input mapped output count differs");
                }
                var resources = new java.util.ArrayList<>(old.resources());
                var planRoot = Sha256Digest.hash(
                        com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.encodePlan(plan));
                for (int i = 0; i < count; i++) {
                    var credit = com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.decode(
                            blob(map, MAX_SOURCE_MAP_BYTES));
                    boolean payload = i < plan.outputParts().size();
                    int ordinal = payload ? i : i - plan.outputParts().size();
                    String key = payload
                            ? plan.outputParts().get(ordinal).objectKey()
                            : plan.indexes().get(ordinal).objectKey();
                    var role = payload
                            ? com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.Role.PAYLOAD
                            : com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.Role.INDEX;
                    if (credit.state() != com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.State.ASSIGNED
                            || credit.resource().isEmpty()
                            || credit.output().isEmpty()
                            || !credit.worker()
                                    .identity()
                                    .equals(plan.sourceCut().identity())
                            || !credit.worker().planRootSha256().equals(planRoot)
                            || !credit.worker().taskIdSha256().equals(plan.taskIdSha256())
                            || credit.slot().role() != role
                            || credit.slot().ordinal() != ordinal
                            || !credit.slot().objectKey().equals(key)) {
                        throw new IllegalArgumentException("Native input mapped original credit differs");
                    }
                    resources.add(credit.resource().orElseThrow());
                }
                end(map);
                var ranges = new java.util.ArrayList<>(old.rewrites());
                ranges.addAll(plan.sourceCut()
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .ranges());
                return new NativeProjection(
                        nativeView, java.util.List.copyOf(ranges), java.util.List.copyOf(resources));
            }
        }
    }

    private static void requireLedgerBoundary(CanonicalBytes source, int kind, M5PulsarCoverageV2.ChainLedger ledger)
            throws IOException {
        try (var in = input(source)) {
            long id;
            long last;
            boolean active;
            if (kind == 1) {
                if (!text(blob(in, 128)).equals("NEREUS_V2_NATIVE_PULSAR_BK_READ_SOURCE_V1")) {
                    throw new IllegalArgumentException("Native input BK source domain differs");
                }
                var namespace = PhysicalNamespaceAuthorityBindingV2.decode(blob(in, 512));
                var member = com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2.decode(blob(in, 2048));
                if (!(member instanceof com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger bk)
                        || !bk.namespace().equals(namespace.physicalNamespace())
                        || in.readLong() < 0) {
                    throw new IllegalArgumentException("Native input BK source identity/version differs");
                }
                id = bk.ledgerId();
                int flag = in.readUnsignedByte();
                if (flag > 1) {
                    throw new IllegalArgumentException("Native input BK active flag differs");
                }
                active = flag == 1;
                last = in.readLong();
                if (!active && in.readLong() < 0) {
                    throw new IllegalArgumentException("Native input BK sealed length differs");
                }
                end(in);
            } else {
                if (in.readInt() != 0x4e505256) {
                    throw new IllegalArgumentException("Native input Object source type differs");
                }
                int version = in.readInt();
                if (version != 1 && version != 2) {
                    throw new IllegalArgumentException("Native input Object source version differs");
                }
                String domain = version == 1
                        ? "NEREUS_V2_NATIVE_PULSAR_READ_VIEW_V1"
                        : "NEREUS_V2_NATIVE_PULSAR_OBJECT_READ_VIEW_V1";
                if (!text(blob(in, 128)).equals(domain)) {
                    throw new IllegalArgumentException("Native input Object source domain differs");
                }
                exact(in, 4 * Sha256Digest.LENGTH);
                blob(in, 8);
                blob(in, 8);
                blob(in, 512);
                blob(in, 512);
                id = in.readLong();
                last = in.readLong();
                active = false;
                // The complete remaining immutable source is committed by the exact directory hash above.
            }
            if (id != ledger.ledgerId()
                    || last < -1
                    || Math.addExact(last, 1) != ledger.entryEndExclusive()
                    || active != ledger.active()) {
                throw new IllegalArgumentException("Native input ledger boundary/active state differs");
            }
        }
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(2);
                out.write(metadataNamespace.encode().toByteArray());
                write(out, CanonicalUtf8.fromString(authorityRoot).bytes());
                write(out, CanonicalUtf8.fromString(aggregateKey).bytes());
                out.writeLong(aggregateVersion);
                write(out, aggregateValue);
                write(out, CanonicalUtf8.fromString(selectorKey).bytes());
                out.writeLong(selectorVersion);
                write(out, selectorValue);
                write(out, CanonicalUtf8.fromString(sourceViewKey).bytes());
                out.writeLong(sourceViewVersion);
                out.write(sourceViewValueSha256.bytes().toByteArray());
                write(out, coverage.encode());
                out.write(recoveryInputSha256.bytes().toByteArray());
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("Native input body exceeds its byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode Native input", impossible);
        }
    }

    public Sha256Digest protocolStateRootSha256() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                write(
                        out,
                        CanonicalUtf8.fromString("NEREUS_V2_M5_PULSAR_NATIVE_INPUT_V2")
                                .bytes());
                write(out, encode());
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot commit Native input", impossible);
        }
    }

    public static OxiaPulsarMaterializationInputV2 decode(CanonicalBytes bytes) {
        if (bytes == null || bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Native input body length differs");
        }
        try (var in = input(bytes)) {
            if (in.readInt() != MAGIC || in.readInt() != 2) {
                throw new IllegalArgumentException("Native input type/version differs");
            }
            var result = new OxiaPulsarMaterializationInputV2(
                    MetadataNamespaceIdentityV2.decode(
                            CanonicalBytes.copyOf(exact(in, MetadataNamespaceIdentityV2.IDENTITY_BYTES))),
                    text(blob(in, MAX_KEY_BYTES)),
                    text(blob(in, MAX_KEY_BYTES)),
                    in.readLong(),
                    blob(in, Nta1CodecV1.MAX_NTA1_BYTES),
                    text(blob(in, MAX_KEY_BYTES)),
                    in.readLong(),
                    blob(in, com.nereusstream.metadata.oxia.v2.codec.Nps1SelectorAuthorityCodec.MAX_BYTES),
                    text(blob(in, MAX_KEY_BYTES)),
                    in.readLong(),
                    digest(in),
                    M5PulsarCoverageV2.decode(blob(in, M5PulsarCoverageV2.MAX_BYTES)),
                    digest(in));
            end(in);
            if (!result.encode().equals(bytes)) {
                throw new IllegalArgumentException("Native input is not canonical");
            }
            return result;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Native input is truncated", failure);
        }
    }

    private static DataInputStream input(CanonicalBytes bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private static void end(DataInputStream in) throws IOException {
        if (in.read() != -1) {
            throw new IllegalArgumentException("Native input has trailing bytes");
        }
    }

    private static byte[] exact(DataInputStream in, int length) throws IOException {
        var bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static CanonicalBytes blob(DataInputStream in, int maximum) throws IOException {
        int size = in.readInt();
        if (size < 1 || size > maximum || size > in.available()) {
            throw new IllegalArgumentException("Native input field length exceeds its exact byte cap");
        }
        return CanonicalBytes.copyOf(exact(in, size));
    }

    private static void write(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static Sha256Digest digest(DataInputStream in) throws IOException {
        return Sha256Digest.copyOf(exact(in, Sha256Digest.LENGTH));
    }

    private static String text(CanonicalBytes bytes) {
        return CanonicalUtf8.fromBytes(bytes.toByteArray()).value();
    }

    private static void requireText(String value) {
        if (value == null
                || value.isEmpty()
                || value.indexOf('\0') >= 0
                || CanonicalUtf8.fromString(value).bytes().length() > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("Native input text exceeds its byte cap");
        }
    }

    private static void requireDigest(Sha256Digest value) {
        if (Objects.requireNonNull(value, "Native input digest").isZero()) {
            throw new IllegalArgumentException("Native input digest is zero");
        }
    }
}
