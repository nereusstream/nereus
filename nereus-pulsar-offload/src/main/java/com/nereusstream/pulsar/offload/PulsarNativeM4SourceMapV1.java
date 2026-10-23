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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedObjectReadIdentity;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedReadSourceIdentity;

/** Full immutable actual view bytes. Loading them grants no Native actor, reconstructed SDK handle or deletion. */
public final class PulsarNativeM4SourceMapV1 {
    private static final int MAGIC = 0x4e50534d; // NPSM
    public static final int MAX_BYTES = 1_048_576;
    private static final int HEADER_BYTES = 44;
    private final CanonicalBytes actualView;
    private final Sha256Digest viewSha;

    private PulsarNativeM4SourceMapV1(CanonicalBytes actualView, Sha256Digest viewSha) {
        if (actualView.isEmpty()
                || actualView.length() > MAX_BYTES - HEADER_BYTES
                || !Sha256Digest.hash(actualView).equals(viewSha)) {
            throw new IllegalArgumentException("Native source-map bytes differ from their selected view digest");
        }
        this.actualView = actualView;
        this.viewSha = viewSha;
    }

    static PulsarNativeM4SourceMapV1 capture(PulsarNativeBindingReadViewV1 view) {
        return new PulsarNativeM4SourceMapV1(view.canonicalBytes(), view.sha256());
    }

    public Sha256Digest selectedViewSha256() {
        return viewSha;
    }

    public CanonicalBytes actualViewBytes() {
        return actualView;
    }

    /** Resolves the exact protected source from these immutable bytes, independently of a later source cut. */
    List<PhysicalResourceIdV2> protectedPhysicalReferences(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects,
            Sha256Digest sourceIdentity) {
        if (PulsarNativeMaterializedReadV2.isRecipe(actualView)) {
            return PulsarNativeMaterializedReadV2.decodeRecipe(actualView)
                    .map()
                    .protectedPhysicalReferences(binding, bookKeeper, objects, sourceIdentity);
        }
        var directory = floorDirectory(binding);
        if (!directory.bookKeeper().equals(bookKeeper) || !directory.objects().equals(objects)) {
            throw new IllegalArgumentException("Native protected source belongs to another physical namespace");
        }
        var row = directory.chain().stream()
                .filter(value -> value.sourceIdentitySha256().equals(sourceIdentity))
                .findFirst();
        if (row.isEmpty()) {
            return List.of();
        }
        var selected = row.orElseThrow();
        if (selected.active() || selected.entryEndExclusive() == 0) {
            throw new IllegalArgumentException("Native protected source is not a whole sealed ledger");
        }
        return nativeLedgerReferences(
                nativeSource(binding, objects, selected.ledgerId()).orElseThrow(),
                binding,
                bookKeeper,
                objects,
                selected.ledgerId(),
                selected.entryEndExclusive());
    }

    record FloorDirectory(
            List<M5PulsarCoverageV2.ChainLedger> chain,
            Map<Long, PublishedObject> objectOnly,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects) {}

    /** Complete original directory for a selected Native view; loading it grants no new read actor. */
    List<M5PulsarCoverageV2.ChainLedger> floorChain(PulsarNativeBindingV1 binding) {
        return floorDirectory(binding).chain();
    }

    FloorDirectory floorDirectory(PulsarNativeBindingV1 binding) {
        try (var in = input(actualView)) {
            if (in.readInt() != 0x4e504256 || in.readInt() != 1) {
                throw new IllegalArgumentException("Native floor source view format differs");
            }
            domain(in, "NEREUS_V2_NATIVE_PULSAR_BINDING_READ_VIEW_V1");
            requireBinding(in, binding);
            requireSha(in, binding.authority().aggregate().canonicalStoredDigest());
            blob(in);
            blob(in);
            var objects = PhysicalNamespaceAuthorityBindingV2.decode(blob(in));
            if (!objects.metadataNamespace().equals(binding.metadataNamespace())) {
                throw new IllegalArgumentException("Native floor Object namespace differs from Binding");
            }
            nonnegative(in.readLong());
            var info = new org.apache.bookkeeper.mledger.impl.MetaStoreImpl(null, null)
                    .parseManagedLedgerInfo(blob(in).toByteArray(), MAX_BYTES);
            binding.requireStoredMetadataBinding(info);
            nonnegative(in.readLong());
            if (in.readLong() < -1) {
                throw new IllegalArgumentException("Native floor confirmed position differs");
            }
            int count = count(in, M5PulsarCoverageV2.MAX_LEDGERS);
            if (count == 0 || count != info.getLedgerInfosList().size()) {
                throw new IllegalArgumentException("Native floor source directory is incomplete");
            }
            var chain = new ArrayList<M5PulsarCoverageV2.ChainLedger>(count);
            var objectOnly = new HashMap<Long, PublishedObject>();
            PhysicalNamespaceAuthorityBindingV2 originalBookKeeper = null;
            long previous = -1;
            for (int index = 0; index < count; index++) {
                long ledger = in.readLong();
                int kind = in.readUnsignedByte();
                var source = blob(in);
                if (ledger <= previous
                        || ledger != info.getLedgerInfosList().get(index).getLedgerId()) {
                    throw new IllegalArgumentException("Native floor source directory order differs");
                }
                previous = ledger;
                final PhysicalNamespaceAuthorityBindingV2 bookKeeper;
                final long last;
                final boolean active;
                try (var raw = input(source)) {
                    if (kind == 1) {
                        domain(raw, "NEREUS_V2_NATIVE_PULSAR_BK_READ_SOURCE_V1");
                        bookKeeper = PhysicalNamespaceAuthorityBindingV2.decode(blob(raw));
                        var plain = validatePlain(source, ledger, bookKeeper);
                        last = plain.lastEntry();
                        active = plain.sealedLength().isEmpty();
                    } else if (kind == 2) {
                        if (raw.readInt() != 0x4e505256) {
                            throw new IllegalArgumentException("Native floor offload source format differs");
                        }
                        int version = raw.readInt();
                        if (version != 1 && version != 2) {
                            throw new IllegalArgumentException("Native floor offload source version differs");
                        }
                        domain(
                                raw,
                                version == 1
                                        ? "NEREUS_V2_NATIVE_PULSAR_READ_VIEW_V1"
                                        : "NEREUS_V2_NATIVE_PULSAR_OBJECT_READ_VIEW_V1");
                        requireBinding(raw, binding);
                        requireSha(raw, binding.authority().aggregate().canonicalStoredDigest());
                        blob(raw);
                        blob(raw);
                        bookKeeper = PhysicalNamespaceAuthorityBindingV2.decode(blob(raw));
                        requireNamespace(blob(raw), objects);
                        if (raw.readLong() != ledger) {
                            throw new IllegalArgumentException("Native floor offload ledger differs");
                        }
                        last = nonnegative(raw.readLong());
                        active = false;
                        // Existing full decoder checks all fields, exact members, immutable versions and suffix.
                        nativeLedgerReferences(source, binding, bookKeeper, objects, ledger, Math.addExact(last, 1));
                        if (version == 2) {
                            objectOnly.put(ledger, readObject(source, ledger, binding, bookKeeper, objects));
                        }
                    } else {
                        throw new IllegalArgumentException("Native floor source kind differs");
                    }
                }
                if (!bookKeeper.metadataNamespace().equals(binding.metadataNamespace())
                        || originalBookKeeper != null && !originalBookKeeper.equals(bookKeeper)
                        || active && index != count - 1) {
                    throw new IllegalArgumentException("Native floor source namespace or active-tail order differs");
                }
                originalBookKeeper = bookKeeper;
                chain.add(new M5PulsarCoverageV2.ChainLedger(
                        ledger, active, Math.addExact(last, 1), Sha256Digest.hash(source)));
            }
            end(in);
            return new FloorDirectory(List.copyOf(chain), Map.copyOf(objectOnly), originalBookKeeper, objects);
        } catch (Exception failure) {
            throw new IllegalArgumentException("Native floor source view cannot be validated", failure);
        }
    }

    /** Resolve one selected slice from full immutable source bytes, including retained predecessor payloads. */
    List<PhysicalResourceIdV2> physicalDataReferences(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects,
            EntryRange range) {
        if (!bookKeeper.metadataNamespace().equals(binding.metadataNamespace())
                || !objects.metadataNamespace().equals(binding.metadataNamespace())) {
            throw new IllegalArgumentException("published physical references belong to another metadata namespace");
        }
        var recipe = PulsarNativeMaterializedReadV2.isRecipe(actualView)
                ? PulsarNativeMaterializedReadV2.decodeRecipe(actualView)
                : null;
        if (recipe != null) {
            if (!recipe.map().plan().sourceCut().identity().binding().equals(binding.bindingIdentity())
                    || recipe.map().splitAtSourceBoundaries(range).size() != 1) {
                throw new IllegalArgumentException("published physical reference crosses its actual source boundary");
            }
            var selected = recipe.map().locate(new Position(range.ledgerId(), range.startEntryInclusive()));
            recipe.map().locate(new Position(range.ledgerId(), range.endEntryExclusive() - 1));
            if (selected.rewrittenPayload().isPresent()) {
                var resource = selected.rewrittenPayload().orElseThrow();
                if (!resource.namespace().equals(objects.physicalNamespace())) {
                    throw new IllegalArgumentException(
                            "published mapped payload belongs to another physical namespace");
                }
                return List.of(resource);
            }
            return nativeDataReferences(
                    selected.retainedNativeSource().orElseThrow(), binding, bookKeeper, objects, range);
        }
        return nativeDataReferences(
                nativeSource(binding, objects, range.ledgerId())
                        .orElseThrow(() ->
                                new IllegalArgumentException("published physical reference has no Native source")),
                binding,
                bookKeeper,
                objects,
                range);
    }

    /** Exact physical members for a whole retained metadata row; no logical-ID substitution for mapped entries. */
    java.util.Optional<List<PhysicalResourceIdV2>> physicalLedgerReferences(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects,
            long ledgerId,
            long entryCount) {
        if (entryCount < 0
                || !bookKeeper.metadataNamespace().equals(binding.metadataNamespace())
                || !objects.metadataNamespace().equals(binding.metadataNamespace())) {
            throw new IllegalArgumentException("published ledger references differ from actual metadata/namespace");
        }
        if (PulsarNativeMaterializedReadV2.isRecipe(actualView)) {
            return PulsarNativeMaterializedReadV2.decodeRecipe(actualView)
                    .map()
                    .physicalLedgerReferences(binding, bookKeeper, objects, ledgerId, entryCount);
        }
        return nativeSource(binding, objects, ledgerId)
                .map(source -> nativeLedgerReferences(source, binding, bookKeeper, objects, ledgerId, entryCount));
    }

    private java.util.Optional<CanonicalBytes> nativeSource(
            PulsarNativeBindingV1 binding, PhysicalNamespaceAuthorityBindingV2 objects, long ledgerId) {
        try (var in = input(actualView)) {
            if (in.readInt() != 0x4e504256 || in.readInt() != 1) {
                throw new IllegalArgumentException("published Native Binding view format differs");
            }
            domain(in, "NEREUS_V2_NATIVE_PULSAR_BINDING_READ_VIEW_V1");
            requireBinding(in, binding);
            requireSha(in, binding.authority().aggregate().canonicalStoredDigest());
            blob(in);
            blob(in);
            requireNamespace(blob(in), objects);
            nonnegative(in.readLong());
            blob(in);
            nonnegative(in.readLong());
            if (in.readLong() < -1) {
                throw new IllegalArgumentException("published Native frontier differs");
            }
            int count = count(in, 512);
            CanonicalBytes selected = null;
            long previous = -1;
            for (int i = 0; i < count; i++) {
                long ledger = in.readLong();
                int kind = in.readUnsignedByte();
                var source = blob(in);
                if (ledger <= previous || kind < 1 || kind > 2) {
                    throw new IllegalArgumentException("published Native source order/type differs");
                }
                previous = ledger;
                if (ledger == ledgerId) {
                    selected = source;
                }
            }
            end(in);
            return java.util.Optional.ofNullable(selected);
        } catch (IOException failure) {
            throw new IllegalArgumentException("published physical reference source is truncated", failure);
        }
    }

    static List<PhysicalResourceIdV2> nativeLedgerReferences(
            CanonicalBytes source,
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects,
            long ledgerId,
            long entryCount) {
        try (var in = input(source)) {
            if (in.readInt() != 0x4e505256) {
                var plain = validatePlain(source, ledgerId, bookKeeper);
                if (plain.sealedLength().isPresent() && entryCount > Math.addExact(plain.lastEntry(), 1)) {
                    throw new IllegalArgumentException("published sealed ledger reference exceeds its frontier");
                }
                return List.of(plain.member());
            }
            if (entryCount == 0) {
                throw new IllegalArgumentException("published sealed offload reference has no entries");
            }
            return nativeDataReferences(source, binding, bookKeeper, objects, new EntryRange(ledgerId, 0, entryCount));
        } catch (IOException invalid) {
            throw new IllegalArgumentException("published Native ledger source is truncated", invalid);
        }
    }

    private static List<PhysicalResourceIdV2> nativeDataReferences(
            CanonicalBytes source,
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects,
            EntryRange range) {
        try (var in = input(source)) {
            if (in.readInt() != 0x4e505256) {
                var plain = validatePlain(source, range.ledgerId(), bookKeeper);
                if (range.endEntryExclusive() - 1 > plain.lastEntry()) {
                    throw new IllegalArgumentException("published plain reference exceeds its actual frontier");
                }
                return List.of(plain.member());
            }
            int version = in.readInt();
            if (version != 1 && version != 2) {
                throw new IllegalArgumentException("published offload reference source version differs");
            }
            domain(
                    in,
                    version == 1
                            ? "NEREUS_V2_NATIVE_PULSAR_READ_VIEW_V1"
                            : "NEREUS_V2_NATIVE_PULSAR_OBJECT_READ_VIEW_V1");
            requireBinding(in, binding);
            requireSha(in, binding.authority().aggregate().canonicalStoredDigest());
            blob(in);
            blob(in);
            requireNamespace(blob(in), bookKeeper);
            requireNamespace(blob(in), objects);
            if (in.readLong() != range.ledgerId() || nonnegative(in.readLong()) < range.endEntryExclusive() - 1) {
                throw new IllegalArgumentException("published offload reference exceeds its actual source");
            }
            nonnegative(in.readLong());
            nonnegative(in.readLong());
            in.readLong();
            in.readLong();
            if (!bit(in)) {
                throw new IllegalArgumentException("published offload reference is incomplete");
            }
            int delete = in.readUnsignedByte();
            int priority = in.readUnsignedByte();
            if (delete < 1 || delete > 3 || priority < 1 || priority > 2) {
                throw new IllegalArgumentException("published offload reference flags differ");
            }
            blob(in);
            properties(in);
            properties(in);
            var resources = new ArrayList<PhysicalResourceIdV2>();
            if (version == 1) {
                nonnegative(in.readLong());
            }
            if (in.readInt() != (version == 1 ? 3 : 2)) {
                throw new IllegalArgumentException("published offload physical reference count differs");
            }
            if (version == 1) {
                var resource = PhysicalResourceIdCodecV2.decode(blob(in));
                if (!(resource instanceof PhysicalResourceIdV2.BookKeeperLedger bk)
                        || bk.ledgerId() != range.ledgerId()
                        || !bk.namespace().equals(bookKeeper.physicalNamespace())) {
                    throw new IllegalArgumentException(
                            "published offload BK fallback differs from its actual namespace");
                }
                resources.add(resource);
            }
            resources.add(objectMember(blob(in), objects));
            resources.add(objectMember(blob(in), objects));
            nonnegative(in.readLong());
            readSha(in);
            nonnegative(in.readLong());
            readSha(in);
            end(in);
            return resources.stream().sorted().distinct().toList();
        } catch (IOException failure) {
            throw new IllegalArgumentException("published Native physical references are truncated", failure);
        }
    }

    record PublishedObject(
            OffloadedReadSourceIdentity identity,
            long lastEntry,
            long logicalLength,
            Map<String, String> driverMetadata) {}

    record PublishedPlain(PhysicalResourceIdV2.BookKeeperLedger member, long lastEntry, OptionalLong sealedLength) {}

    record PublishedSources(Map<Long, PublishedObject> objects, Map<Long, PublishedPlain> plain) {}

    /** Extracts proofs from the SHA-bound full source bytes, never a separately supplied member summary. */
    Map<Long, PublishedObject> publishedObjects(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects) {
        return publishedSources(binding, bookKeeper, objects).objects();
    }

    PublishedSources publishedSources(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects) {
        var nativeBytes = PulsarNativeMaterializedReadV2.isRecipe(actualView)
                ? PulsarNativeMaterializedReadV2.decodeRecipe(actualView).nativeView()
                : actualView;
        try (var in = input(nativeBytes)) {
            if (in.readInt() != 0x4e504256 || in.readInt() != 1) {
                throw new IllegalArgumentException("published Native Binding view format differs");
            }
            domain(in, "NEREUS_V2_NATIVE_PULSAR_BINDING_READ_VIEW_V1");
            requireBinding(in, binding);
            requireSha(in, binding.authority().aggregate().canonicalStoredDigest());
            blob(in);
            blob(in); // Exact P1 version tokens remain in the full SHA-bound source.
            requireNamespace(blob(in), objects);
            nonnegative(in.readLong());
            blob(in);
            nonnegative(in.readLong());
            if (in.readLong() < -1) {
                throw new IllegalArgumentException("published Native frontier differs");
            }
            int count = count(in, 512);
            var result = new HashMap<Long, PublishedObject>();
            var plain = new HashMap<Long, PublishedPlain>();
            long previous = -1;
            for (int i = 0; i < count; i++) {
                long ledger = in.readLong();
                if (ledger <= previous) {
                    throw new IllegalArgumentException("published Native ledgers are not ordered");
                }
                previous = ledger;
                int kind = in.readUnsignedByte();
                var source = blob(in);
                if (kind == 1) {
                    plain.put(ledger, validatePlain(source, ledger, bookKeeper));
                } else if (kind == 2) {
                    result.put(ledger, readObject(source, ledger, binding, bookKeeper, objects));
                } else {
                    throw new IllegalArgumentException("published Native source kind differs");
                }
            }
            end(in);
            return new PublishedSources(Map.copyOf(result), Map.copyOf(plain));
        } catch (IOException failure) {
            throw new IllegalArgumentException("published Native source view is truncated", failure);
        }
    }

    private static PublishedObject readObject(
            CanonicalBytes bytes,
            long ledger,
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects)
            throws IOException {
        try (var in = input(bytes)) {
            if (in.readInt() != 0x4e505256 || in.readInt() != 2) {
                throw new IllegalArgumentException("cold published source is not Object-only");
            }
            domain(in, "NEREUS_V2_NATIVE_PULSAR_OBJECT_READ_VIEW_V1");
            requireBinding(in, binding);
            requireSha(in, binding.authority().aggregate().canonicalStoredDigest());
            blob(in);
            blob(in);
            requireNamespace(blob(in), bookKeeper);
            requireNamespace(blob(in), objects);
            if (in.readLong() != ledger) {
                throw new IllegalArgumentException("published Object ledger differs");
            }
            long last = nonnegative(in.readLong());
            long length = nonnegative(in.readLong());
            nonnegative(in.readLong());
            var attempt = new UUID(in.readLong(), in.readLong());
            if (!bit(in)) {
                throw new IllegalArgumentException("published offload is incomplete");
            }
            int delete = in.readUnsignedByte();
            int priority = in.readUnsignedByte();
            if (delete < 1 || delete > 3 || priority < 1 || priority > 2) {
                throw new IllegalArgumentException("published Native source flags differ");
            }
            blob(in);
            properties(in);
            var driver = properties(in);
            if (in.readInt() != 2) {
                throw new IllegalArgumentException("published Object members differ");
            }
            var rootMember = objectMember(blob(in), objects);
            var dataMember = objectMember(blob(in), objects);
            var root = new OffloadedObjectReadIdentity(
                    rootMember.objectKey().value(),
                    "version:" + rootMember.immutableIdentity().value(),
                    nonnegative(in.readLong()),
                    readSha(in).toHex());
            var data = new OffloadedObjectReadIdentity(
                    dataMember.objectKey().value(),
                    "version:" + dataMember.immutableIdentity().value(),
                    nonnegative(in.readLong()),
                    readSha(in).toHex());
            end(in);
            return new PublishedObject(
                    new OffloadedReadSourceIdentity(ledger, attempt, root, data), last, length, driver);
        }
    }

    private static PublishedPlain validatePlain(
            CanonicalBytes bytes, long ledger, PhysicalNamespaceAuthorityBindingV2 namespace) throws IOException {
        try (var in = input(bytes)) {
            domain(in, "NEREUS_V2_NATIVE_PULSAR_BK_READ_SOURCE_V1");
            requireNamespace(blob(in), namespace);
            var member = PhysicalResourceIdCodecV2.decode(blob(in));
            if (!(member instanceof PhysicalResourceIdV2.BookKeeperLedger bk)
                    || bk.ledgerId() != ledger
                    || !bk.namespace().equals(namespace.physicalNamespace())) {
                throw new IllegalArgumentException("published plain BK member differs");
            }
            nonnegative(in.readLong());
            boolean active = bit(in);
            long lastEntry = in.readLong();
            if (lastEntry < -1) {
                throw new IllegalArgumentException("published plain extent differs");
            }
            var length = active ? OptionalLong.empty() : OptionalLong.of(nonnegative(in.readLong()));
            end(in);
            return new PublishedPlain(bk, lastEntry, length);
        }
    }

    private static PhysicalResourceIdV2.ObjectVersion objectMember(
            CanonicalBytes bytes, PhysicalNamespaceAuthorityBindingV2 expected) {
        var resource = PhysicalResourceIdCodecV2.decode(bytes);
        if (!(resource instanceof PhysicalResourceIdV2.ObjectVersion object)
                || object.identityKind() != PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION
                || !object.namespace().equals(expected.physicalNamespace())) {
            throw new IllegalArgumentException("published immutable Object member differs");
        }
        return object;
    }

    private static void requireBinding(DataInputStream in, PulsarNativeBindingV1 binding) throws IOException {
        var identity = binding.bindingIdentity();
        requireSha(in, identity.bindingId().digest());
        requireSha(in, identity.incarnationSha256());
        requireSha(in, identity.storageEpochSha256());
    }

    private static void requireNamespace(CanonicalBytes bytes, PhysicalNamespaceAuthorityBindingV2 expected) {
        if (!PhysicalNamespaceAuthorityBindingV2.decode(bytes).equals(expected)) {
            throw new IllegalArgumentException("published Native namespace differs from actual backend");
        }
    }

    private static Map<String, String> properties(DataInputStream in) throws IOException {
        int count = count(in, 4096);
        var result = new HashMap<String, String>();
        byte[] previous = null;
        for (int i = 0; i < count; i++) {
            byte[] key = blob(in).toByteArray();
            if (previous != null && Arrays.compareUnsigned(previous, key) >= 0) {
                throw new IllegalArgumentException("published Native properties are not ordered");
            }
            previous = key;
            result.put(text(CanonicalBytes.copyOf(key)), text(blob(in)));
        }
        return Map.copyOf(result);
    }

    private static int count(DataInputStream in, int maximum) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > maximum) {
            throw new IllegalArgumentException("published Native count differs");
        }
        return count;
    }

    private static CanonicalBytes blob(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_BYTES || length > in.available()) {
            throw new IllegalArgumentException("published Native blob length differs");
        }
        return CanonicalBytes.copyOf(in.readNBytes(length));
    }

    private static void domain(DataInputStream in, String expected) throws IOException {
        if (!blob(in).equals(CanonicalBytes.copyOf(expected.getBytes(StandardCharsets.UTF_8)))) {
            throw new IllegalArgumentException("published Native domain differs");
        }
    }

    private static DataInputStream input(CanonicalBytes bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private static boolean bit(DataInputStream in) throws IOException {
        int value = in.readUnsignedByte();
        if (value > 1) {
            throw new IllegalArgumentException("published Native boolean is not canonical");
        }
        return value == 1;
    }

    private static String text(CanonicalBytes bytes) {
        String value = new String(bytes.toByteArray(), StandardCharsets.UTF_8);
        if (!CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)).equals(bytes)) {
            throw new IllegalArgumentException("published Native UTF-8 is malformed");
        }
        return value;
    }

    private static long nonnegative(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("published Native scalar is negative");
        }
        return value;
    }

    private static void end(DataInputStream in) throws IOException {
        if (in.available() != 0) {
            throw new IllegalArgumentException("published Native source has trailing bytes");
        }
    }

    private static Sha256Digest readSha(DataInputStream in) throws IOException {
        byte[] bytes = new byte[32];
        in.readFully(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private static void requireSha(DataInputStream in, Sha256Digest expected) throws IOException {
        if (!readSha(in).equals(expected)) {
            throw new IllegalArgumentException("published Native Binding/P1 digest differs");
        }
    }

    public CanonicalBytes encode() {
        return CanonicalBytes.copyOf(ByteBuffer.allocate(HEADER_BYTES + actualView.length())
                .putInt(MAGIC)
                .putInt(1)
                .put(viewSha.bytes().toByteArray())
                .putInt(actualView.length())
                .put(actualView.toByteArray())
                .array());
    }

    public static PulsarNativeM4SourceMapV1 decode(CanonicalBytes encoded, Sha256Digest expectedView) {
        if (encoded.length() <= HEADER_BYTES || encoded.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Native source-map length is outside its bound");
        }
        var bytes = encoded.toByteArray();
        var in = ByteBuffer.wrap(bytes);
        if (in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("Native source-map magic/version differs");
        }
        byte[] digest = new byte[32];
        in.get(digest);
        int length = in.getInt();
        if (!Sha256Digest.copyOf(digest).equals(expectedView) || length != in.remaining()) {
            throw new IllegalArgumentException("Native source map does not match the actual selector view");
        }
        return new PulsarNativeM4SourceMapV1(
                CanonicalBytes.copyOf(Arrays.copyOfRange(bytes, HEADER_BYTES, bytes.length)), expectedView);
    }
}
