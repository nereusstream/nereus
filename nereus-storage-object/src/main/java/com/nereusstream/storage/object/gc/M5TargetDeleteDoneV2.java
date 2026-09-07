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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.DeleteTerminalOutcomeV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteDoneV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Permanent compact terminal at the original physical resource key; it grants no new dispatch or admission. */
public record M5TargetDeleteDoneV2(
        PhysicalResourceIdV2 resource,
        long authorityRevision,
        Sha256Digest fullDoneAuthoritySha256,
        long closedWriterFenceEpoch,
        Sha256Digest eligibilitySha256,
        Sha256Digest finalCapabilitySha256,
        Sha256Digest finalDispatchTokenSha256,
        Sha256Digest externalIdentitySha256,
        TargetDeleteDoneV1 done) {
    private static final int MAGIC = 0x4d354443; // M5DC
    public static final int MAX_BYTES = PhysicalResourceIdV2.MAX_ENCODED_BYTES + 512;

    public M5TargetDeleteDoneV2 {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(done, "done");
        M5TargetDeleteAuthorityRecordsV1.requirePositive(closedWriterFenceEpoch, "closedWriterFenceEpoch");
        if (authorityRevision < 3 || done.intentAuthorityRevision() >= authorityRevision - 1) {
            throw new IllegalArgumentException("compact done must follow a full done revision after its intent");
        }
        requireDigest(fullDoneAuthoritySha256);
        requireDigest(eligibilitySha256);
        requireDigest(finalCapabilitySha256);
        requireDigest(finalDispatchTokenSha256);
        requireDigest(externalIdentitySha256);
    }

    public String authorityKey() {
        return resource.authorityKey();
    }

    /** Only an exact full DELETE_DONE can be compacted, preserving its chain and fixed completion identities. */
    public static M5TargetDeleteDoneV2 from(TargetDeleteAuthorityV1 authority) {
        var full = M5TargetDeleteAuthorityCodecV1.encodeAuthority(authority);
        if (authority.state() != TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
            throw new IllegalArgumentException("compact done requires exact full DELETE_DONE_V1");
        }
        var intent = authority.deleteIntent().orElseThrow();
        return new M5TargetDeleteDoneV2(
                authority.target().resourceId(),
                Math.addExact(authority.authorityRevision(), 1),
                Sha256Digest.hash(full),
                authority.closedWriterFenceEpoch(),
                authority.proofSnapshotDigest(),
                intent.capabilityDigestSha256(),
                intent.dispatchTokenSha256(),
                authority.externalIdentity().orElseThrow().externalIdentitySha256(),
                authority.deleteDone().orElseThrow());
    }

    public static boolean isCompactDone(CanonicalBytes bytes) {
        return bytes.length() >= Integer.BYTES
                && ByteBuffer.wrap(bytes.toByteArray()).getInt() == MAGIC;
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(2);
            var identity = resource.canonicalBytes();
            out.writeInt(identity.length());
            out.write(identity.toByteArray());
            out.writeLong(authorityRevision);
            digest(out, fullDoneAuthoritySha256);
            out.writeLong(closedWriterFenceEpoch);
            digest(out, eligibilitySha256);
            digest(out, finalCapabilitySha256);
            digest(out, finalDispatchTokenSha256);
            digest(out, externalIdentitySha256);
            digest(out, done.intentCanonicalSha256());
            out.writeLong(done.intentAuthorityRevision());
            digest(out, done.deleteAttemptIdSha256());
            out.writeLong(done.finalDispatchEpoch());
            digest(out, done.finalDispatchOwnerFenceSha256());
            out.writeInt(done.terminalOutcome().ordinal());
            digest(out, done.absenceInventoryRootSha256());
            digest(out, done.completionProofDigestSha256());
            out.flush();
            digest(out, Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray())));
            out.flush();
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("compact done exceeds its encoded byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static M5TargetDeleteDoneV2 decode(CanonicalBytes bytes) {
        if (bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("compact done byte length differs");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
            if (in.readInt() != MAGIC || in.readInt() != 2) {
                throw new IllegalArgumentException("compact done magic or version differs");
            }
            int length = in.readInt();
            if (length <= 0 || length > PhysicalResourceIdV2.MAX_ENCODED_BYTES || length > in.available()) {
                throw new IllegalArgumentException("compact done resource byte bound differs");
            }
            var resource = PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(in.readNBytes(length)));
            long revision = in.readLong();
            var predecessor = digest(in);
            long fence = in.readLong();
            var eligibility = digest(in);
            var capability = digest(in);
            var dispatch = digest(in);
            var external = digest(in);
            var intent = digest(in);
            long intentRevision = in.readLong();
            var attempt = digest(in);
            long dispatchEpoch = in.readLong();
            var owner = digest(in);
            int outcome = in.readInt();
            if (outcome < 0 || outcome >= DeleteTerminalOutcomeV1.values().length) {
                throw new IllegalArgumentException("compact done terminal outcome differs");
            }
            var done = new TargetDeleteDoneV1(
                    intent,
                    intentRevision,
                    attempt,
                    dispatchEpoch,
                    owner,
                    DeleteTerminalOutcomeV1.values()[outcome],
                    digest(in),
                    digest(in));
            var value = new M5TargetDeleteDoneV2(
                    resource, revision, predecessor, fence, eligibility, capability, dispatch, external, done);
            digest(in);
            if (in.read() != -1 || !value.encode().equals(bytes)) {
                throw new IllegalArgumentException("compact done canonical bytes or checksum differ");
            }
            return value;
        } catch (IOException invalid) {
            throw new IllegalArgumentException("compact done is truncated", invalid);
        }
    }

    private static void digest(DataOutputStream out, Sha256Digest value) throws IOException {
        out.write(value.bytes().toByteArray());
    }

    private static Sha256Digest digest(DataInputStream in) throws IOException {
        return Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH));
    }

    private static void requireDigest(Sha256Digest value) {
        M5TargetDeleteAuthorityRecordsV1.requireDigest(value, "compact done identity");
    }
}
