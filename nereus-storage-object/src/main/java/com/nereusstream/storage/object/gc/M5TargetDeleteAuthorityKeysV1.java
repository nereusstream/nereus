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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetKindV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Domain-separated target identities, authority keys, and dispatch-token roots for M5-D. */
public final class M5TargetDeleteAuthorityKeysV1 {
    private static final CanonicalBytes TARGET_DOMAIN =
            CanonicalUtf8.fromString("NEREUS_V2_M5_TARGET_DELETE_IDENTITY_V1").bytes();
    private static final CanonicalBytes EXTERNAL_IDENTITY_DOMAIN =
            CanonicalUtf8.fromString("NEREUS_V2_M5_EXTERNAL_DELETE_IDENTITY_V1").bytes();
    private static final CanonicalBytes DISPATCH_TOKEN_DOMAIN =
            CanonicalUtf8.fromString("NEREUS_V2_M5_DELETE_DISPATCH_TOKEN_V1").bytes();

    private M5TargetDeleteAuthorityKeysV1() {}

    public static Sha256Digest targetIdentitySha256(
            CellProviderScopeId cellProviderScopeId,
            PhysicalDeleteTargetKindV1 targetKind,
            CanonicalBytes exactTargetIdentity) {
        Objects.requireNonNull(cellProviderScopeId, "cellProviderScopeId");
        Objects.requireNonNull(targetKind, "targetKind");
        M5TargetDeleteAuthorityRecordsV1.requireBytes(
                exactTargetIdentity, M5TargetDeleteAuthorityRecordsV1.MAX_TARGET_IDENTITY_BYTES, "exactTargetIdentity");
        return hash(output -> {
            writeBytes(output, TARGET_DOMAIN);
            writeDigest(output, cellProviderScopeId.digest());
            output.writeByte(targetKind.ordinal());
            writeBytes(output, exactTargetIdentity);
        });
    }

    public static String authorityKey(PhysicalDeleteTargetV1 target) {
        Objects.requireNonNull(target, "target");
        String key = "v2/physical-delete-m5/"
                + target.cellProviderScopeId().digest().toHex()
                + "/"
                + target.targetIdentitySha256().toHex()
                + "/authority-v1";
        if (CanonicalUtf8.fromString(key).bytes().length() > ExactMetadataTransactionStoreV1.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("target delete authority key exceeds the metadata key hard cap");
        }
        return key;
    }

    public static Sha256Digest externalIdentitySha256(
            PhysicalDeleteTargetKindV1 targetKind,
            ExternalIdentityObservationV1 observation,
            CanonicalBytes exactExternalIdentity) {
        Objects.requireNonNull(targetKind, "targetKind");
        Objects.requireNonNull(observation, "observation");
        M5TargetDeleteAuthorityRecordsV1.requireBytes(
                exactExternalIdentity,
                M5TargetDeleteAuthorityRecordsV1.MAX_EXTERNAL_IDENTITY_BYTES,
                "exactExternalIdentity");
        return hash(output -> {
            writeBytes(output, EXTERNAL_IDENTITY_DOMAIN);
            output.writeByte(targetKind.ordinal());
            output.writeByte(observation.ordinal());
            writeBytes(output, exactExternalIdentity);
        });
    }

    public static Sha256Digest dispatchTokenSha256(
            String authorityKey,
            Sha256Digest targetIdentitySha256,
            long intentAuthorityRevision,
            Sha256Digest deleteAttemptIdSha256,
            long dispatchEpoch,
            Sha256Digest dispatchOwnerFenceSha256,
            Sha256Digest externalIdentitySha256) {
        M5TargetDeleteAuthorityRecordsV1.requireKey(authorityKey);
        M5TargetDeleteAuthorityRecordsV1.requireDigest(targetIdentitySha256, "targetIdentitySha256");
        M5TargetDeleteAuthorityRecordsV1.requirePositive(intentAuthorityRevision, "intentAuthorityRevision");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(deleteAttemptIdSha256, "deleteAttemptIdSha256");
        M5TargetDeleteAuthorityRecordsV1.requirePositive(dispatchEpoch, "dispatchEpoch");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(dispatchOwnerFenceSha256, "dispatchOwnerFenceSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(externalIdentitySha256, "externalIdentitySha256");
        return hash(output -> {
            writeBytes(output, DISPATCH_TOKEN_DOMAIN);
            writeBytes(output, CanonicalUtf8.fromString(authorityKey).bytes());
            writeDigest(output, targetIdentitySha256);
            output.writeLong(intentAuthorityRevision);
            writeDigest(output, deleteAttemptIdSha256);
            output.writeLong(dispatchEpoch);
            writeDigest(output, dispatchOwnerFenceSha256);
            writeDigest(output, externalIdentitySha256);
        });
    }

    private static Sha256Digest hash(Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writer.write(output);
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException exception) {
            throw new IllegalStateException("failed to create M5-D domain-separated identity", exception);
        }
    }

    private static void writeBytes(DataOutputStream output, CanonicalBytes value) throws IOException {
        output.writeInt(value.length());
        output.write(value.toByteArray());
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest value) throws IOException {
        output.write(value.bytes().toByteArray());
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }
}
