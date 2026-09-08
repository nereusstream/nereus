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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.gc.DeleteExternalIdentityReaderV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Actual sealed BK metadata observations through an instance-bound client. This class never dispatches delete. */
public final class KafkaBookKeeperDeleteIdentityReaderV2 implements DeleteExternalIdentityReaderV2 {
    private final M5BookKeeperNativeCreateClientV2 client;
    private final RunLedgerHandleV1 handle;
    private final PhysicalResourceIdV2.BookKeeperLedger resource;

    public KafkaBookKeeperDeleteIdentityReaderV2(M5BookKeeperNativeCreateClientV2 client, RunLedgerHandleV1 handle) {
        this.client = Objects.requireNonNull(client, "client");
        this.handle = Objects.requireNonNull(handle, "handle");
        var capability = client.capabilitySnapshot();
        if (!handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())
                || !client.spec()
                        .configurations()
                        .contains(RunLedgerConfigurationV1.from(capability, handle.runId()))) {
            throw new IllegalArgumentException("delete identity handle is outside the admitted native BK scope");
        }
        resource = new PhysicalResourceIdV2.BookKeeperLedger(
                client.spec().namespace(), handle.ledgerIdentity().ledgerId());
    }

    /** Used between CAS-1 and CAS-2. The coordinator separately revalidates native owner/eligibility authority. */
    public CompletionStage<ExactExternalIdentityV1> capture(TargetDeleteAuthorityV1 fenced) {
        if (!fenced.target().resourceId().equals(resource)
                || fenced.state() != TargetDeleteAuthorityStateV1.READ_FENCED_V1
                || fenced.recoveryVeto().isPresent()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("capture requires this exact read fence"));
        }
        return client.captureExactTarget(handle).thenApply(result -> switch (result.outcome()) {
            case EXACT_TARGET ->
                ExactExternalIdentityV1.create(
                        fenced, ExternalIdentityObservationV1.PRESENT_EXACT_V1, encodeIdentity(result.exactTarget()));
            case DEFINITIVELY_ABSENT ->
                ExactExternalIdentityV1.create(
                        fenced, ExternalIdentityObservationV1.ABSENT_EXACT_V1, encodeIdentity(Optional.empty()));
            case DIFFERENT_OR_UNSEALED, OUTCOME_UNKNOWN ->
                throw new IllegalStateException("native BK identity is changed, unsealed or unknown");
        });
    }

    @Override
    public CompletionStage<ExternalIdentityObservationV1> rereadExact(ExactExternalIdentityV1 expected) {
        Objects.requireNonNull(expected, "expected");
        if (!expected.resourceId().equals(resource)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("native BK namespace/ledger differs"));
        }
        return client.captureExactTarget(handle).thenApply(result -> switch (result.outcome()) {
            case EXACT_TARGET -> {
                if (expected.observation() != ExternalIdentityObservationV1.PRESENT_EXACT_V1
                        || !expected.exactIdentityBytes().equals(encodeIdentity(result.exactTarget()))) {
                    throw new IllegalStateException("native BK sealed identity changed or an absent ledger reappeared");
                }
                yield ExternalIdentityObservationV1.PRESENT_EXACT_V1;
            }
            // Native metadata absence identifies this immutable ledger in this actual instance. No cached
            // fingerprint or caller-supplied absence statement can produce this branch.
            case DEFINITIVELY_ABSENT -> ExternalIdentityObservationV1.ABSENT_EXACT_V1;
            case DIFFERENT_OR_UNSEALED, OUTCOME_UNKNOWN ->
                throw new IllegalStateException("native BK identity is changed, unsealed or unknown");
        });
    }

    private CanonicalBytes encodeIdentity(Optional<BookKeeperDeleteTargetV1> target) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(0x4d354249); // M5BI
                out.writeInt(1);
                writeBytes(out, resource.canonicalBytes());
                writeBytes(out, handle.providerScopeId().digest().bytes());
                writeBytes(out, handle.runId().value().bytes());
                writeBytes(out, handle.configurationDigest().bytes());
                out.writeBoolean(target.isPresent());
                if (target.isPresent()) {
                    var exact = target.orElseThrow();
                    out.writeLong(exact.sealedLastEntryId());
                    out.writeLong(exact.sealedLength());
                    out.writeInt(exact.ensembleSize());
                    out.writeInt(exact.writeQuorumSize());
                    out.writeInt(exact.ackQuorumSize());
                    writeBytes(
                            out,
                            CanonicalUtf8.fromString(exact.digestType().name()).bytes());
                    writeBytes(
                            out,
                            CanonicalUtf8.fromString(exact.passwordCredentialIdentityVersion())
                                    .bytes());
                    writeBytes(out, exact.passwordSha256().bytes());
                    out.writeInt(exact.metadataFormatVersion());
                    out.writeLong(exact.metadataCToken());
                    // Includes all sorted custom metadata, every ensemble, ctime, state and native identity fields.
                    writeBytes(out, exact.metadataSha256().bytes());
                }
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory native BK identity encoding failed", impossible);
        }
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }
}
