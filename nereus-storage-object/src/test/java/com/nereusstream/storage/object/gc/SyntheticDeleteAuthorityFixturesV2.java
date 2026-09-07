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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.DeleteTerminalOutcomeV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import java.util.ArrayList;
import java.util.List;

/** Synthetic deletion proofs for unit/native metadata-storage tests only; no actual Provider/BK delete is certified. */
public final class SyntheticDeleteAuthorityFixturesV2 {
    private SyntheticDeleteAuthorityFixturesV2() {}

    public static PhysicalResourceIdV2 resource(int id) {
        return new PhysicalResourceIdV2.ObjectVersion(
                new PhysicalResourceIdV2.Namespace(
                        PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER,
                        CanonicalUtf8.fromString("synthetic-provider-instance"),
                        CanonicalUtf8.fromString("synthetic-bucket-instance")),
                CanonicalUtf8.fromString("object-" + id),
                PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION,
                CanonicalUtf8.fromString("version-1"));
    }

    public static List<TargetDeleteAuthorityV1> phases(PhysicalResourceIdV2 resource) {
        var open = M5TargetDeleteAuthorityStateMachineV1.open(
                PhysicalDeleteTargetV1.create(resource),
                new ProofBoundWriterEnrollmentV1(
                        List.of(ProofBoundWriterClassV1.values()),
                        digest("writer-set"),
                        digest("admission"),
                        digest("native-synthetic")),
                M5DeleteEligibilityTestFixtures.replacement(resource, 1));
        var fenced = M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(
                open, digest("read"), M5DeleteEligibilityTestFixtures.observation());
        var external = ExactExternalIdentityV1.create(
                fenced,
                ExternalIdentityObservationV1.PRESENT_EXACT_V1,
                CanonicalUtf8.fromString("synthetic immutable version metadata").bytes());
        var intent = M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(fenced, external, digest("delete"));
        var done = M5TargetDeleteAuthorityStateMachineV1.completeDelete(
                intent,
                DeleteTerminalOutcomeV1.DELETED_EXACT_V1,
                digest("synthetic absence"),
                digest("synthetic completion"));
        return List.of(open, fenced, intent, done);
    }

    public static List<VersionedValue> facts(PhysicalResourceIdV2 resource) {
        var values = new ArrayList<>(M5DeleteEligibilityTestFixtures.metadataValues(
                phases(resource).get(0).eligibilitySnapshot().orElseThrow()));
        for (var fact : M5DeleteEligibilityTestFixtures.observation().authorityFacts()) {
            values.add(VersionedValue.of(
                    fact.key(), CanonicalUtf8.fromString(fact.key()).bytes(), fact.metadataVersion()));
        }
        return List.copyOf(values);
    }

    public static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }
}
