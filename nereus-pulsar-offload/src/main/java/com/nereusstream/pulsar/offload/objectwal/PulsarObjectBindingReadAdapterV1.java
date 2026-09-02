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

package com.nereusstream.pulsar.offload.objectwal;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentLocator;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ManifestSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadProtocolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.BindingReadRouteV1.SourcePurity;
import com.nereusstream.storage.object.read.BindingReadSourceRefV1;
import com.nereusstream.storage.object.read.BindingReadSourceRefV1.SourceKind;
import com.nereusstream.storage.object.read.PulsarBindingReadRouteTableV1;
import com.nereusstream.storage.object.read.PulsarBindingReadRouteV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Projects the actual P4 Object-WAL view into one immutable M4 Pulsar publication cell. */
public final class PulsarObjectBindingReadAdapterV1 {
    public record PhysicalRoute(PulsarBindingReadRouteV1 route, ReadSource source) {
        public PhysicalRoute {
            Objects.requireNonNull(route, "route");
            Objects.requireNonNull(source, "source");
        }
    }

    public record ReadCell(
            PulsarObjectWalReadViewV1 view, PulsarBindingReadRouteTableV1 routes, List<PhysicalRoute> physicalRoutes) {
        public ReadCell {
            Objects.requireNonNull(view, "view");
            Objects.requireNonNull(routes, "routes");
            physicalRoutes = List.copyOf(Objects.requireNonNull(physicalRoutes, "physicalRoutes"));
            if (routes.size() != physicalRoutes.size()) {
                throw new IllegalArgumentException("Pulsar M4 logical and physical route counts differ");
            }
            for (int index = 0; index < routes.size(); index++) {
                if (!routes.route(index).equals(physicalRoutes.get(index).route())) {
                    throw new IllegalArgumentException("Pulsar M4 logical and physical route order differs");
                }
            }
        }

        public PhysicalRoute requirePhysical(PulsarBindingReadRouteV1 route) {
            return physicalRoutes.stream()
                    .filter(candidate -> candidate.route().equals(route))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("planned Pulsar route is absent from read cell"));
        }
    }

    private PulsarObjectBindingReadAdapterV1() {}

    public static BindingIdentity bindingIdentity(PulsarBindingKey binding) {
        Objects.requireNonNull(binding, "binding");
        return new BindingIdentity(
                new TopicBindingId(textDigest("M4-PULSAR-BINDING-ID-V1", binding.topicBindingId())),
                textDigest("M4-PULSAR-INCARNATION-V1", binding.topicIncarnation()),
                textDigest("M4-PULSAR-STORAGE-EPOCH-V1", binding.storageEpochId()));
    }

    public static BindingReadAuthorityV1 publish(PulsarObjectWalReadViewV1 view, BindingReadSelector selector) {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(selector, "selector");
        BindingIdentity expected = bindingIdentity(view.binding());
        if (!selector.binding().equals(expected) || selector.ownerEpoch() != view.ownerEpoch()) {
            throw new IllegalArgumentException("Pulsar P4 view differs from the exact M4 selector authority");
        }

        List<PulsarBindingReadRouteV1> logical = new ArrayList<>();
        List<PhysicalRoute> physical = new ArrayList<>();
        long maximumReadableUpperBound = 0;
        for (PulsarObjectWalReadViewV1.LedgerView ledger : view.ledgers()) {
            maximumReadableUpperBound = Math.max(maximumReadableUpperBound, Math.addExact(ledger.readableThrough(), 1));
            for (PulsarObjectWalReadViewV1.SourceInterval interval : ledger.intervals()) {
                BindingReadSourceRefV1 source = sourceRef(view.binding(), interval);
                PulsarBindingReadRouteV1 route = new PulsarBindingReadRouteV1(
                        interval.virtualLedgerId(),
                        interval.startEntryIdInclusive(),
                        interval.endEntryIdExclusive(),
                        source,
                        null,
                        0,
                        SourcePurity.PULSAR_ENTRY);
                logical.add(route);
                physical.add(new PhysicalRoute(route, interval.source()));
            }
        }
        PulsarBindingReadRouteTableV1 routes = new PulsarBindingReadRouteTableV1(logical);
        ReadCell readCell = new ReadCell(view, routes, physical);
        BindingReadPublicationCellV1 publication = new BindingReadPublicationCellV1(
                selector.sourceGeneration(),
                maximumReadableUpperBound,
                view.viewVersion(),
                new BindingReadRouteTableV1(List.of()),
                readCell);
        return new BindingReadAuthorityV1(
                expected.bindingId(),
                expected.incarnationSha256(),
                new StorageEpochId(expected.storageEpochSha256()),
                BindingReadProtocolV1.PULSAR_LEDGER_ENTRY,
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                selector.admissionState()
                        == com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState.ADMITTING,
                selector.capability().generation(),
                selector.capability().evidenceSha256(),
                publication);
    }

    private static BindingReadSourceRefV1 sourceRef(
            PulsarBindingKey binding, PulsarObjectWalReadViewV1.SourceInterval interval) {
        Sha256Digest sourceIdentity;
        Sha256Digest sourceVersion;
        if (interval.source() == ReadSource.MANIFEST) {
            ManifestSource manifest = interval.manifest().orElseThrow();
            sourceIdentity = manifestDigest(manifest);
            sourceVersion = manifest.objectIdentity().bodySha256();
        } else {
            ExtentLocator locator = interval.activeLocator().orElseThrow();
            sourceIdentity = locatorDigest(locator);
            sourceVersion = locator.identity().objectIdentity().bodySha256();
        }
        Sha256Digest semantic = digest("M4-PULSAR-ENTRY-SEMANTIC-V1", out -> {
            writeBinding(out, binding);
            out.writeLong(interval.virtualLedgerId());
            out.writeLong(interval.startEntryIdInclusive());
            out.writeLong(interval.endEntryIdExclusive());
        });
        return new BindingReadSourceRefV1(SourceKind.OBJECT, sourceIdentity, sourceVersion, semantic, 0);
    }

    private static Sha256Digest manifestDigest(ManifestSource manifest) {
        return digest("M4-PULSAR-MANIFEST-SOURCE-V1", out -> {
            writeObjectIdentity(out, manifest.objectIdentity());
            out.writeLong(manifest.authorityVersion());
            writeText(out, manifest.authorityProofSha256());
        });
    }

    private static Sha256Digest locatorDigest(ExtentLocator locator) {
        return digest("M4-PULSAR-ACTIVE-LOCATOR-V1", out -> {
            writeBinding(out, locator.binding());
            out.writeLong(locator.position().virtualLedgerId());
            out.writeLong(locator.position().entryId());
            out.write(locator.identity().walRunRootSha256().bytes().toByteArray());
            out.writeInt(locator.identity().laneId().ordinal());
            out.writeLong(locator.identity().laneSequence());
            writeObjectIdentity(out, locator.identity().objectIdentity());
            out.writeInt(locator.frameOrdinal());
            out.writeLong(locator.extentOffset());
            out.writeLong(locator.extentLength());
            writeText(out, locator.frameSha256());
        });
    }

    private static Sha256Digest textDigest(String tag, String value) {
        return digest(tag, out -> writeText(out, value));
    }

    private static void writeBinding(DataOutputStream out, PulsarBindingKey binding) throws IOException {
        writeText(out, binding.protocolCellId());
        writeText(out, binding.topicBindingId());
        writeText(out, binding.topicIncarnation());
        writeText(out, binding.storageEpochId());
        writeText(out, binding.positionDomainId());
    }

    private static void writeObjectIdentity(DataOutputStream out, ObjectIdentity identity) throws IOException {
        writeText(out, identity.key());
        out.writeLong(identity.bodyLength());
        out.write(identity.bodySha256().bytes().toByteArray());
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static Sha256Digest digest(String tag, Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writeText(out, tag);
                writer.write(out);
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Pulsar M4 identity encoding failed", impossible);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }
}
