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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.VerifiedPulsarOutputs;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Complete Binding routing data for one independently verified rewrite. Outside-cut native sources remain exact.
 * Neither decoding this value nor locating an entry grants a reader, M4 publication, fallback release or deletion.
 */
public final class PulsarNativeMaterializationReadMapV2 {
    public static final int MAX_BYTES = 1_048_576;
    public static final int MAX_PREDECESSOR_MAPS = 64;
    private static final int MAGIC = 0x4e504d4d; // NPMM
    private final PulsarNativeM4SourceMapV1 predecessor;
    private final MaterializationPlan plan;
    private final List<M5ObjectBirthCreditV3> outputs;
    private final Map<Long, CanonicalBytes> nativeSources;
    private final PulsarNativeMaterializationReadMapV2 priorMap;
    private final CanonicalBytes bytes;

    private PulsarNativeMaterializationReadMapV2(
            PulsarNativeM4SourceMapV1 predecessor, MaterializationPlan plan, List<M5ObjectBirthCreditV3> outputs) {
        this(predecessor, plan, outputs, 0);
    }

    private PulsarNativeMaterializationReadMapV2(
            PulsarNativeM4SourceMapV1 predecessor,
            MaterializationPlan plan,
            List<M5ObjectBirthCreditV3> outputs,
            int depth) {
        if (depth > MAX_PREDECESSOR_MAPS) {
            throw new IllegalArgumentException("Native rewrite predecessor maps exceed their depth bound");
        }
        this.predecessor = predecessor;
        this.plan = plan;
        this.outputs = List.copyOf(outputs);
        var cut = plan.sourceCut();
        if (!cut.coverage().typedPulsar()
                || plan.representationMode() != RepresentationMode.REWRITE_GENERATION
                || !cut.predecessorSelector().selectedViewSha256().equals(predecessor.selectedViewSha256())
                || !cut.coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .chainAuthorityRootSha256()
                        .equals(predecessor.selectedViewSha256())
                || this.outputs.size()
                        != plan.outputParts().size() + plan.indexes().size()) {
            throw new IllegalArgumentException("Native rewrite map differs from its complete original cut");
        }
        var root = Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan));
        var sourceRoot = M5MaterializationCodecV1.sourceCutSha256(cut);
        for (int i = 0; i < this.outputs.size(); i++) {
            var credit = this.outputs.get(i);
            boolean payload = i < plan.outputParts().size();
            int ordinal = payload ? i : i - plan.outputParts().size();
            String key = payload
                    ? plan.outputParts().get(ordinal).objectKey()
                    : plan.indexes().get(ordinal).objectKey();
            var worker = credit.worker();
            if (credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED
                    || credit.output().isEmpty()
                    || !credit.slot()
                            .equals(new M5PendingOutputInventoryV2.Slot(
                                    payload
                                            ? M5PendingOutputInventoryV2.Role.PAYLOAD
                                            : M5PendingOutputInventoryV2.Role.INDEX,
                                    ordinal,
                                    key))
                    || !worker.identity().equals(cut.identity())
                    || !worker.taskIdSha256().equals(plan.taskIdSha256())
                    || !worker.planRootSha256().equals(root)
                    || !worker.predecessorSelectorSha256()
                            .equals(Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(cut.predecessorSelector())))
                    || !worker.sourceCutSha256().equals(sourceRoot)
                    || !worker.equals(this.outputs.get(0).worker())
                    || !credit.scope().equals(this.outputs.get(0).scope())) {
                throw new IllegalArgumentException("Native rewrite map lacks its exact original assigned output");
            }
        }
        var recipe = PulsarNativeMaterializedReadV2.isRecipe(predecessor.actualViewBytes())
                ? PulsarNativeMaterializedReadV2.decodeRecipe(predecessor.actualViewBytes(), depth + 1)
                : null;
        priorMap = recipe == null ? null : recipe.map();
        nativeSources = originalSources(predecessor, plan, this.outputs, recipe);
        bytes = encodeBounded();
    }

    /** The original Native Task supplies actual assigned credits after full SDK/output verification. */
    static PulsarNativeMaterializationReadMapV2 capture(
            PulsarNativeBindingReadViewV1 view, VerifiedPulsarOutputs verified, List<M5ObjectBirthCreditV3> credits) {
        var descriptors = java.util.stream.Stream.concat(verified.payloads().stream(), verified.indexes().stream())
                .toList();
        if (!view.isCurrent() || descriptors.size() != credits.size()) {
            throw new IllegalArgumentException("Native rewrite map requires its current original source/output proof");
        }
        var result = new PulsarNativeMaterializationReadMapV2(
                PulsarNativeM4SourceMapV1.capture(view), verified.plan(), credits);
        for (int i = 0; i < descriptors.size(); i++) {
            var descriptor = descriptors.get(i);
            var credit = credits.get(i);
            var resource = credit.resource().orElseThrow();
            if (!credit.output().equals(Optional.of(descriptor.identity()))
                    || !resource.namespace().equals(view.objectNamespace())
                    || !CanonicalUtf8.fromString(resource.immutableIdentity().value())
                            .bytes()
                            .equals(descriptor.immutableProviderVersionToken().orElseThrow())) {
                throw new IllegalArgumentException("Native rewrite map substituted a verified physical output");
            }
        }
        return result;
    }

    public MaterializationPlan plan() {
        return plan;
    }

    /** Rebuild routing from selected immutable metadata; this is data, not a recovered SDK actor or reader grant. */
    static PulsarNativeMaterializationReadMapV2 restore(
            PulsarNativeM4SourceMapV1 predecessor,
            MaterializationPlan plan,
            List<M5ObjectBirthCreditV3> credits,
            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializedGeneration
                    generation) {
        var result = new PulsarNativeMaterializationReadMapV2(predecessor, plan, credits);
        var descriptors = java.util.stream.Stream.concat(
                        generation.payloadObjects().stream(), generation.indexObjects().stream())
                .toList();
        if (descriptors.size() != credits.size()) {
            throw new IllegalArgumentException("selected generation dropped an assigned Native output");
        }
        for (int i = 0; i < descriptors.size(); i++) {
            var descriptor = descriptors.get(i);
            var credit = credits.get(i);
            boolean payload = i < plan.outputParts().size();
            var coverage = payload
                    ? plan.outputParts().get(i).coverage()
                    : plan.indexes().get(i - plan.outputParts().size()).coverage();
            var kind = payload
                    ? null
                    : plan.indexes().get(i - plan.outputParts().size()).kind();
            if (descriptor.ordinal() != credit.slot().ordinal()
                    || !descriptor.coverage().equals(coverage)
                    || descriptor.indexKind() != kind
                    || !credit.output().equals(Optional.of(descriptor.identity()))
                    || !descriptor
                            .immutableProviderVersionToken()
                            .equals(Optional.of(CanonicalUtf8.fromString(credit.resource()
                                            .orElseThrow()
                                            .immutableIdentity()
                                            .value())
                                    .bytes()))) {
                throw new IllegalArgumentException(
                        "selected Native output descriptor differs from its assigned version");
            }
        }
        return result;
    }

    public PulsarNativeM4SourceMapV1 predecessor() {
        return predecessor;
    }

    public List<M5ObjectBirthCreditV3> outputs() {
        return outputs;
    }

    /** Original Native records still supplying entries outside the selected rewrite ranges. */
    PulsarNativeM4SourceMapV1.FloorDirectory originalNativeDirectory(PulsarNativeBindingV1 binding) {
        return priorMap == null ? predecessor.floorDirectory(binding) : priorMap.originalNativeDirectory(binding);
    }

    MaterializationPlan planForOutput(M5ObjectBirthCreditV3 credit) {
        if (outputs.contains(credit)) {
            return plan;
        }
        if (priorMap == null) {
            throw new IllegalArgumentException("Native mapped payload has no original assigned plan");
        }
        return priorMap.planForOutput(credit);
    }

    /** Current outputs plus predecessor payloads still selected outside this exact cut. */
    List<M5ObjectBirthCreditV3> readableOutputs() {
        var result = new java.util.ArrayList<>(outputs);
        if (priorMap != null) {
            for (var credit : priorMap.readableOutputs()) {
                if (credit.slot().role() == M5PendingOutputInventoryV2.Role.PAYLOAD && stillSelected(credit)) {
                    result.add(credit);
                }
            }
        }
        return List.copyOf(result.stream().distinct().toList());
    }

    List<M5PulsarCoverageV2.EntryRange> selectedRanges(M5ObjectBirthCreditV3 credit) {
        for (int i = 0; i < plan.outputParts().size(); i++) {
            if (outputs.get(i).equals(credit)) {
                return plan.outputParts()
                        .get(i)
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .ranges();
            }
        }
        return priorMap == null ? List.of() : priorMap.selectedRanges(credit);
    }

    private boolean stillSelected(M5ObjectBirthCreditV3 credit) {
        var chain = plan.sourceCut().coverage().pulsarCoverage().orElseThrow().chain();
        for (var range : priorMap.selectedRanges(credit)) {
            var ledger = chain.stream()
                    .filter(value -> value.ledgerId() == range.ledgerId())
                    .findFirst();
            if (ledger.isEmpty()
                    || range.startEntryInclusive() >= ledger.orElseThrow().entryEndExclusive()) {
                continue;
            }
            var retained = new M5PulsarCoverageV2.EntryRange(
                    range.ledgerId(),
                    range.startEntryInclusive(),
                    Math.min(range.endEntryExclusive(), ledger.orElseThrow().entryEndExclusive()));
            for (var slice : splitAtSourceBoundaries(retained)) {
                if (locate(new M5PulsarCoverageV2.Position(slice.ledgerId(), slice.startEntryInclusive()))
                        .rewrittenPayload()
                        .equals(credit.resource())) {
                    return true;
                }
            }
        }
        return false;
    }

    void requireSuccessorCapacity() {
        int predecessors = 1;
        for (var old = priorMap; old != null; old = old.priorMap) {
            predecessors++;
        }
        if (predecessors >= MAX_PREDECESSOR_MAPS) {
            throw new IllegalStateException("Native next rewrite exceeds its admitted predecessor-map capacity");
        }
    }

    long decodedWorkspaceBytes() {
        long bytes = 16L * 1024 * 1024;
        for (var map = this; map != null; map = map.priorMap) {
            bytes = Math.addExact(
                    bytes,
                    Math.multiplyExact(
                            4L,
                            Math.addExact(
                                    map.encode().length(),
                                    map.predecessor.encode().length())));
        }
        return bytes;
    }

    static Sha256Digest mappedLedgerRoot(Sha256Digest view, long ledger) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(44)
                .putInt(0x4e504d4c)
                .put(view.bytes().toByteArray())
                .putLong(ledger)
                .array()));
    }

    boolean fullyReplaces(long ledgerId, long lastEntry) {
        if (lastEntry < 0) {
            return false;
        }
        var ranges = new java.util.ArrayList<M5PulsarCoverageV2.EntryRange>();
        collectRewriteRanges(ledgerId, ranges);
        long end = 0;
        for (var range : ranges.stream()
                .sorted(java.util.Comparator.comparingLong(M5PulsarCoverageV2.EntryRange::startEntryInclusive))
                .toList()) {
            if (range.startEntryInclusive() > end) {
                return false;
            }
            end = Math.max(end, range.endEntryExclusive());
        }
        return end == Math.addExact(lastEntry, 1);
    }

    private void collectRewriteRanges(long ledger, List<M5PulsarCoverageV2.EntryRange> ranges) {
        plan.outputParts().stream()
                .flatMap(part -> part.coverage().pulsarCoverage().orElseThrow().ranges().stream())
                .filter(range -> range.ledgerId() == ledger)
                .forEach(ranges::add);
        if (priorMap != null) {
            priorMap.collectRewriteRanges(ledger, ranges);
        }
    }

    List<M5PulsarCoverageV2.EntryRange> splitAtSourceBoundaries(M5PulsarCoverageV2.EntryRange requested) {
        var ranges = new java.util.ArrayList<M5PulsarCoverageV2.EntryRange>();
        collectRewriteRanges(requested.ledgerId(), ranges);
        var boundaries = new java.util.TreeSet<Long>();
        boundaries.add(requested.startEntryInclusive());
        boundaries.add(requested.endEntryExclusive());
        for (var range : ranges) {
            if (range.endEntryExclusive() > requested.startEntryInclusive()
                    && range.startEntryInclusive() < requested.endEntryExclusive()) {
                boundaries.add(Math.max(requested.startEntryInclusive(), range.startEntryInclusive()));
                boundaries.add(Math.min(requested.endEntryExclusive(), range.endEntryExclusive()));
            }
        }
        var result = new java.util.ArrayList<M5PulsarCoverageV2.EntryRange>();
        Long previous = null;
        for (long end : boundaries) {
            if (previous != null) {
                result.add(new M5PulsarCoverageV2.EntryRange(requested.ledgerId(), previous, end));
            }
            previous = end;
        }
        return List.copyOf(result);
    }

    /** Exact predecessor data dependencies of this rewrite's fallback cut, never its preferred output substitutes. */
    List<PhysicalResourceIdV2> fallbackPhysicalReferences(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects) {
        return predecessorPhysicalReferences(
                binding,
                bookKeeper,
                objects,
                plan.sourceCut().coverage().pulsarCoverage().orElseThrow().ranges());
    }

    /** Exact original source extent, including a mapped predecessor's physical dependencies. */
    List<PhysicalResourceIdV2> protectedPhysicalReferences(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects,
            Sha256Digest sourceIdentity) {
        var source = plan.sourceCut().sources().stream()
                .filter(value -> value.sourceIdentitySha256().equals(sourceIdentity))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Native protected source is outside the actual cut"));
        return predecessorPhysicalReferences(
                binding,
                bookKeeper,
                objects,
                source.coverage().pulsarCoverage().orElseThrow().ranges());
    }

    private List<PhysicalResourceIdV2> predecessorPhysicalReferences(
            PulsarNativeBindingV1 binding,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            PhysicalNamespaceAuthorityBindingV2 objects,
            List<M5PulsarCoverageV2.EntryRange> ranges) {
        var result = new java.util.TreeSet<PhysicalResourceIdV2>();
        for (var range : ranges) {
            var slices = priorMap == null ? List.of(range) : priorMap.splitAtSourceBoundaries(range);
            for (var slice : slices) {
                result.addAll(predecessor.physicalDataReferences(binding, bookKeeper, objects, slice));
                if (result.size() > com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS) {
                    throw new IllegalStateException("Native fallback physical dependencies exceed their bound");
                }
            }
        }
        return List.copyOf(result);
    }

    Optional<List<com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2>> physicalLedgerReferences(
            PulsarNativeBindingV1 binding,
            com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 objects,
            long ledgerId,
            long entryCount) {
        if (entryCount < 0
                || !plan.sourceCut().identity().binding().equals(binding.bindingIdentity())
                || !bookKeeper.metadataNamespace().equals(binding.metadataNamespace())
                || !objects.metadataNamespace().equals(binding.metadataNamespace())) {
            throw new IllegalArgumentException("mapped ledger references differ from actual Binding/namespace");
        }
        var ledger = plan.sourceCut().coverage().pulsarCoverage().orElseThrow().chain().stream()
                .filter(row -> row.ledgerId() == ledgerId)
                .findFirst();
        if (ledger.isEmpty()) {
            return Optional.empty();
        }
        var resources = new java.util.TreeSet<com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2>();
        long selectedEnd = Math.min(entryCount, ledger.orElseThrow().entryEndExclusive());
        if (selectedEnd > 0) {
            for (var range : splitAtSourceBoundaries(new M5PulsarCoverageV2.EntryRange(ledgerId, 0, selectedEnd))) {
                var route = locate(new M5PulsarCoverageV2.Position(ledgerId, range.startEntryInclusive()));
                if (route.rewrittenPayload().isPresent()) {
                    var resource = route.rewrittenPayload().orElseThrow();
                    if (!resource.namespace().equals(objects.physicalNamespace())) {
                        throw new IllegalArgumentException("mapped physical reference namespace differs");
                    }
                    resources.add(resource);
                } else {
                    resources.addAll(PulsarNativeM4SourceMapV1.nativeLedgerReferences(
                            route.retainedNativeSource().orElseThrow(),
                            binding,
                            bookKeeper,
                            objects,
                            ledgerId,
                            range.endEntryExclusive()));
                }
            }
        }
        if (entryCount == 0 || entryCount > selectedEnd) {
            resources.addAll(PulsarNativeM4SourceMapV1.nativeLedgerReferences(
                    java.util.Objects.requireNonNull(nativeSources.get(ledgerId), "Native tail source is absent"),
                    binding,
                    bookKeeper,
                    objects,
                    ledgerId,
                    entryCount));
        }
        return Optional.of(List.copyOf(resources));
    }

    boolean rewrites(M5PulsarCoverageV2.Position position) {
        return locate(position).rewrittenPayload().isPresent();
    }

    public CanonicalBytes encode() {
        return bytes;
    }

    public Sha256Digest sha256() {
        return Sha256Digest.hash(bytes);
    }

    /** A payload is selected only inside the exact cut; all other admitted entries retain their original source. */
    public Route locate(M5PulsarCoverageV2.Position position) {
        var coverage = plan.sourceCut().coverage().pulsarCoverage().orElseThrow();
        var ledger = coverage.chain().stream()
                .filter(value -> value.ledgerId() == position.ledgerId())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("entry ledger is outside the complete Native map"));
        if (position.entryId() >= ledger.entryEndExclusive()) {
            throw new IllegalArgumentException("entry exceeds the captured Native frontier");
        }
        for (int i = 0; i < plan.outputParts().size(); i++) {
            var ranges = plan.outputParts()
                    .get(i)
                    .coverage()
                    .pulsarCoverage()
                    .orElseThrow()
                    .ranges();
            if (ranges.stream()
                    .anyMatch(range -> range.ledgerId() == position.ledgerId()
                            && position.entryId() >= range.startEntryInclusive()
                            && position.entryId() < range.endEntryExclusive())) {
                return new Route(
                        ledger.sourceIdentitySha256(),
                        Optional.of(outputs.get(i).resource().orElseThrow()),
                        Optional.empty());
            }
        }
        if (priorMap != null
                && priorMap.plan.sourceCut().coverage().pulsarCoverage().orElseThrow().chain().stream()
                        .anyMatch(old -> old.ledgerId() == position.ledgerId()
                                && position.entryId() < old.entryEndExclusive())) {
            var old = priorMap.locate(position);
            if (old.rewrittenPayload().isPresent()) {
                return old;
            }
        }
        return new Route(
                ledger.sourceIdentitySha256(),
                Optional.empty(),
                Optional.ofNullable(nativeSources.get(position.ledgerId())));
    }

    public record Route(
            Sha256Digest originalSourceSha256,
            Optional<PhysicalResourceIdV2.ObjectVersion> rewrittenPayload,
            Optional<CanonicalBytes> retainedNativeSource) {
        public Route {
            if (rewrittenPayload.isPresent() == retainedNativeSource.isPresent()) {
                throw new IllegalArgumentException("Native map route requires exactly one actual source");
            }
        }
    }

    public static PulsarNativeMaterializationReadMapV2 decode(CanonicalBytes bytes) {
        return decode(bytes, 0);
    }

    static PulsarNativeMaterializationReadMapV2 decode(CanonicalBytes bytes, int depth) {
        if (bytes.length() > MAX_BYTES || bytes.length() < 20) {
            throw new IllegalArgumentException("Native rewrite map exceeds its byte bound");
        }
        try (var in = input(bytes)) {
            if (in.readInt() != MAGIC || in.readInt() != 2) {
                throw new IllegalArgumentException("Native rewrite map magic/version differs");
            }
            var plan = M5MaterializationCodecV1.decodePlan(blob(in));
            var predecessor = PulsarNativeM4SourceMapV1.decode(
                    blob(in), plan.sourceCut().predecessorSelector().selectedViewSha256());
            int count = in.readInt();
            if (count <= 0 || count > M5PendingOutputInventoryV2.MAX_OUTPUTS) {
                throw new IllegalArgumentException("Native rewrite map output count exceeds its bound");
            }
            var credits = new java.util.ArrayList<M5ObjectBirthCreditV3>(count);
            for (int i = 0; i < count; i++) {
                credits.add(M5ObjectBirthCreditV3.decode(blob(in)));
            }
            var result = new PulsarNativeMaterializationReadMapV2(predecessor, plan, credits, depth);
            if (in.read() != -1 || !result.encode().equals(bytes)) {
                throw new IllegalArgumentException("Native rewrite map is non-canonical or has trailing bytes");
            }
            return result;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Native rewrite map is truncated", failure);
        }
    }

    private CanonicalBytes encodeBounded() {
        var planBytes = M5MaterializationCodecV1.encodePlan(plan);
        var predecessorBytes = predecessor.encode();
        var credits = outputs.stream().map(M5ObjectBirthCreditV3::encode).toList();
        long size = 20L + planBytes.length() + predecessorBytes.length();
        for (var credit : credits) {
            size = Math.addExact(size, 4L + credit.length());
        }
        if (size > MAX_BYTES) {
            throw new IllegalArgumentException("complete Native rewrite map exceeds its byte bound");
        }
        try {
            var buffer = new ByteArrayOutputStream(Math.toIntExact(size));
            try (var out = new DataOutputStream(buffer)) {
                out.writeInt(MAGIC);
                out.writeInt(2);
                blob(out, planBytes);
                blob(out, predecessorBytes);
                out.writeInt(credits.size());
                for (var credit : credits) {
                    blob(out, credit);
                }
            }
            return CanonicalBytes.copyOf(buffer.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("cannot encode complete Native rewrite map", failure);
        }
    }

    private static Map<Long, CanonicalBytes> originalSources(
            PulsarNativeM4SourceMapV1 original,
            MaterializationPlan plan,
            List<M5ObjectBirthCreditV3> outputs,
            PulsarNativeMaterializedReadV2.Recipe recipe) {
        var coverage = plan.sourceCut().coverage().pulsarCoverage().orElseThrow();
        try (var in = input(recipe == null ? original.actualViewBytes() : recipe.nativeView())) {
            if (in.readInt() != 0x4e504256
                    || in.readInt() != 1
                    || !blob(in).equals(CanonicalUtf8.fromString("NEREUS_V2_NATIVE_PULSAR_BINDING_READ_VIEW_V1")
                            .bytes())) {
                throw new IllegalArgumentException("Native rewrite map predecessor format differs");
            }
            var binding = coverage.binding();
            requireSha(in, binding.bindingId().digest());
            requireSha(in, binding.incarnationSha256());
            requireSha(in, binding.storageEpochSha256());
            readSha(in); // Exact P1 aggregate digest and versions remain SHA-bound in the complete view.
            blob(in);
            blob(in);
            var objects = PhysicalNamespaceAuthorityBindingV2.decode(blob(in));
            if (outputs.stream()
                    .anyMatch(credit ->
                            !credit.resource().orElseThrow().namespace().equals(objects.physicalNamespace()))) {
                throw new IllegalArgumentException("Native rewrite map assigned outputs belong to another namespace");
            }
            in.readLong();
            blob(in); // Exact native ManagedLedger metadata bytes.
            in.readLong();
            in.readLong();
            int count = in.readInt();
            if (count < 0 || count > coverage.chain().size()) {
                throw new IllegalArgumentException("Native rewrite map source count differs from its chain");
            }
            var sources = new LinkedHashMap<Long, CanonicalBytes>();
            long previous = -1;
            for (int i = 0; i < count; i++) {
                long id = in.readLong();
                if (id <= previous) {
                    throw new IllegalArgumentException("Native rewrite map changed its complete ledger order");
                }
                previous = id;
                var ledger = coverage.chain().stream()
                        .filter(value -> value.ledgerId() == id)
                        .findFirst()
                        .orElseThrow();
                int kind = in.readUnsignedByte();
                var source = blob(in);
                if ((kind != 1 && kind != 2) || !Sha256Digest.hash(source).equals(ledger.sourceIdentitySha256())) {
                    throw new IllegalArgumentException("Native rewrite map changed an original physical source");
                }
                requireBoundary(source, kind, ledger, coverage);
                sources.put(ledger.ledgerId(), source);
            }
            for (var ledger : coverage.chain()) {
                if (!sources.containsKey(ledger.ledgerId())
                        && (recipe == null
                                || ledger.active()
                                || !recipe.map().fullyReplaces(ledger.ledgerId(), ledger.entryEndExclusive() - 1)
                                || !ledger.sourceIdentitySha256()
                                        .equals(mappedLedgerRoot(original.selectedViewSha256(), ledger.ledgerId())))) {
                    throw new IllegalArgumentException("Native rewrite map dropped an outside-cut ledger");
                }
            }
            if (in.read() != -1) {
                throw new IllegalArgumentException("Native rewrite map predecessor has trailing bytes");
            }
            return Map.copyOf(sources);
        } catch (IOException failure) {
            throw new IllegalArgumentException("Native rewrite map predecessor is truncated", failure);
        }
    }

    private static void requireBoundary(
            CanonicalBytes source, int kind, M5PulsarCoverageV2.ChainLedger ledger, M5PulsarCoverageV2 coverage)
            throws IOException {
        try (var in = input(source)) {
            final boolean active;
            final long last;
            if (kind == 1) {
                if (!blob(in).equals(CanonicalUtf8.fromString("NEREUS_V2_NATIVE_PULSAR_BK_READ_SOURCE_V1")
                        .bytes())) {
                    throw new IllegalArgumentException("Native rewrite map BK source domain differs");
                }
                var binding = PhysicalNamespaceAuthorityBindingV2.decode(blob(in));
                var resource = PhysicalResourceIdCodecV2.decode(blob(in));
                if (!(resource instanceof PhysicalResourceIdV2.BookKeeperLedger bk)
                        || bk.ledgerId() != ledger.ledgerId()
                        || !bk.namespace().equals(binding.physicalNamespace())
                        || in.readLong() < 0) {
                    throw new IllegalArgumentException("Native rewrite map BK source identity differs");
                }
                int bit = in.readUnsignedByte();
                if (bit > 1) {
                    throw new IllegalArgumentException("Native rewrite map BK active flag differs");
                }
                active = bit == 1;
                last = in.readLong();
                if ((!active && in.readLong() < 0) || in.read() != -1) {
                    throw new IllegalArgumentException("Native rewrite map BK extent differs");
                }
            } else {
                if (in.readInt() != 0x4e505256) {
                    throw new IllegalArgumentException("Native rewrite map Object source magic differs");
                }
                int version = in.readInt();
                if ((version != 1 && version != 2)
                        || !blob(in).equals(CanonicalUtf8.fromString(
                                        version == 1
                                                ? "NEREUS_V2_NATIVE_PULSAR_READ_VIEW_V1"
                                                : "NEREUS_V2_NATIVE_PULSAR_OBJECT_READ_VIEW_V1")
                                .bytes())) {
                    throw new IllegalArgumentException("Native rewrite map Object source version/domain differs");
                }
                requireSha(in, coverage.binding().bindingId().digest());
                requireSha(in, coverage.binding().incarnationSha256());
                requireSha(in, coverage.binding().storageEpochSha256());
                readSha(in);
                blob(in);
                blob(in);
                PhysicalNamespaceAuthorityBindingV2.decode(blob(in));
                PhysicalNamespaceAuthorityBindingV2.decode(blob(in));
                if (in.readLong() != ledger.ledgerId()) {
                    throw new IllegalArgumentException("Native rewrite map Object ledger differs");
                }
                last = in.readLong();
                active = false;
            }
            if (last < -1 || ledger.active() != active || ledger.entryEndExclusive() != Math.addExact(last, 1)) {
                throw new IllegalArgumentException("Native rewrite map changed an original entry frontier");
            }
        }
    }

    private static DataInputStream input(CanonicalBytes bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private static CanonicalBytes blob(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_BYTES || length > in.available()) {
            throw new IllegalArgumentException("Native rewrite map blob exceeds its byte bound");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return CanonicalBytes.copyOf(bytes);
    }

    private static void blob(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static Sha256Digest readSha(DataInputStream in) throws IOException {
        byte[] bytes = new byte[32];
        in.readFully(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private static void requireSha(DataInputStream in, Sha256Digest expected) throws IOException {
        if (!readSha(in).equals(expected)) {
            throw new IllegalArgumentException("Native rewrite map predecessor belongs to another Binding");
        }
    }
}
