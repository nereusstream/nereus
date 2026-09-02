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

package com.nereusstream.kafka.bookkeeper.object.read;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectStateCodecV1;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadProtocolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.BindingReadRouteV1;
import com.nereusstream.storage.object.read.BindingReadRouteV1.SourcePurity;
import com.nereusstream.storage.object.read.BindingReadSourceRefV1;
import com.nereusstream.storage.object.read.BindingReadSourceRefV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Projects the exact M3 coherent root and active-tail locators into one immutable M4 Kafka cell. */
public final class KafkaObjectBindingReadAdapterV1 {
    public record PhysicalRoute(BindingReadRouteV1 route, KafkaObjectExtentLocatorV1 locator) {
        public PhysicalRoute {
            Objects.requireNonNull(route, "route");
            Objects.requireNonNull(locator, "locator");
            if (route.startInclusive() != locator.startOffset()
                    || route.endExclusive() != locator.endOffsetExclusive()) {
                throw new IllegalArgumentException("Kafka logical route differs from its M3 active locator");
            }
        }
    }

    public record ReadCell(
            KafkaObjectCoherentProtocolSnapshotV1 snapshot,
            BindingReadRouteTableV1 routes,
            List<PhysicalRoute> physicalRoutes) {
        public ReadCell {
            Objects.requireNonNull(snapshot, "snapshot");
            Objects.requireNonNull(routes, "routes");
            physicalRoutes = List.copyOf(Objects.requireNonNull(physicalRoutes, "physicalRoutes"));
            if (routes.size() != physicalRoutes.size()) {
                throw new IllegalArgumentException("Kafka M4 logical and physical route counts differ");
            }
            for (int index = 0; index < routes.size(); index++) {
                if (!routes.route(index).equals(physicalRoutes.get(index).route())) {
                    throw new IllegalArgumentException("Kafka M4 logical and physical route order differs");
                }
            }
        }

        public PhysicalRoute requirePhysical(BindingReadRouteV1 route) {
            return physicalRoutes.stream()
                    .filter(candidate -> candidate.route().equals(route))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("planned Kafka route is absent from read cell"));
        }
    }

    private KafkaObjectBindingReadAdapterV1() {}

    public static BindingIdentity bindingIdentity(KafkaObjectCoherentProtocolSnapshotV1 snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        return new BindingIdentity(
                snapshot.root().fence().bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(
                        snapshot.root().fence().topicIncarnation())),
                snapshot.root().fence().storageEpochId().digest());
    }

    public static BindingReadAuthorityV1 publish(
            KafkaObjectCoherentProtocolSnapshotV1 snapshot, BindingReadSelector selector) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(selector, "selector");
        BindingIdentity expected = bindingIdentity(snapshot);
        if (!selector.binding().equals(expected)
                || selector.ownerEpoch() != snapshot.root().fence().ownerEpoch()
                || !snapshot.root()
                        .references()
                        .activeTail()
                        .contentDigest()
                        .equals(Sha256Digest.hash(KafkaObjectStateCodecV1.activeTail(snapshot.activeTail())))) {
            throw new IllegalArgumentException("Kafka M3 coherent root differs from the exact M4 selector/cell");
        }

        List<BindingReadRouteV1> logical = new ArrayList<>();
        List<PhysicalRoute> physical = new ArrayList<>();
        for (KafkaObjectExtentLocatorV1 locator : snapshot.activeTail().locators()) {
            BindingReadSourceRefV1 source = new BindingReadSourceRefV1(
                    SourceKind.OBJECT,
                    Sha256Digest.hash(KafkaObjectStateCodecV1.locator(locator)),
                    locator.extent().bodySha(),
                    semanticIdentity(locator),
                    0);
            BindingReadRouteV1 route = new BindingReadRouteV1(
                    locator.startOffset(),
                    locator.endOffsetExclusive(),
                    source,
                    null,
                    0,
                    SourcePurity.KAFKA_APPEND_UNIT);
            logical.add(route);
            physical.add(new PhysicalRoute(route, locator));
        }
        BindingReadRouteTableV1 routes = new BindingReadRouteTableV1(logical);
        ReadCell readCell = new ReadCell(snapshot, routes, physical);
        BindingReadPublicationCellV1 publication = new BindingReadPublicationCellV1(
                selector.sourceGeneration(),
                snapshot.root().frontiers().readableEndOffset(),
                snapshot.root().references().activeTail().generation(),
                routes,
                readCell);
        return new BindingReadAuthorityV1(
                expected.bindingId(),
                expected.incarnationSha256(),
                snapshot.root().fence().storageEpochId(),
                BindingReadProtocolV1.KAFKA_OFFSET,
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                selector.admissionState() == AdmissionState.ADMITTING,
                selector.capability().generation(),
                selector.capability().evidenceSha256(),
                publication);
    }

    private static Sha256Digest semanticIdentity(KafkaObjectExtentLocatorV1 locator) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeUTF("M4-KAFKA-APPEND-COVERAGE-V1");
                out.write(locator.binding().bindingId().digest().bytes().toByteArray());
                out.write(locator.binding().topicId().value().bytes().toByteArray());
                out.writeInt(locator.binding().partitionId());
                out.write(locator.binding().storageEpochId().digest().bytes().toByteArray());
                out.writeLong(locator.startOffset());
                out.writeLong(locator.endOffsetExclusive());
                out.write(locator.extent().bodySha().bytes().toByteArray());
                out.writeInt(locator.firstDirectoryRow());
                out.writeInt(locator.directoryRowCount());
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Kafka M4 semantic encoding failed", impossible);
        }
    }
}
