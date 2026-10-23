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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import java.nio.ByteBuffer;
import java.util.Objects;

/** One bounded worker grant, stored on the same authority key as its selector. These bytes alone grant no admission. */
public record M5MaterializationWorkerV1(
        IdentityEnvelope identity,
        Sha256Digest nativeWorkerIdentitySha256,
        Sha256Digest nativeNamespaceBindingSha256,
        Sha256Digest predecessorSelectorSha256,
        Sha256Digest sourceCutSha256,
        Sha256Digest planRootSha256,
        Sha256Digest taskIdSha256) {
    private static final int MAGIC = 0x4d355743; // M5WC
    public static final int BYTES = 8 + 4 * Long.BYTES + 12 * Sha256Digest.LENGTH;

    public M5MaterializationWorkerV1 {
        Objects.requireNonNull(identity, "identity");
        requireDigest(nativeWorkerIdentitySha256);
        requireDigest(nativeNamespaceBindingSha256);
        requireDigest(predecessorSelectorSha256);
        requireDigest(sourceCutSha256);
        requireDigest(planRootSha256);
        requireDigest(taskIdSha256);
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(BYTES).putInt(MAGIC).putInt(1);
        digest(out, identity.protocolCellSha256());
        digest(out, identity.providerScopeSha256());
        digest(out, identity.binding().bindingId().digest());
        digest(out, identity.binding().incarnationSha256());
        digest(out, identity.binding().storageEpochSha256());
        out.putLong(identity.ownerEpoch()).putLong(identity.workerEpoch()).putLong(identity.storageFence());
        out.putLong(identity.capability().generation());
        digest(out, identity.capability().evidenceSha256());
        digest(out, nativeWorkerIdentitySha256);
        digest(out, nativeNamespaceBindingSha256);
        digest(out, predecessorSelectorSha256);
        digest(out, sourceCutSha256);
        digest(out, planRootSha256);
        digest(out, taskIdSha256);
        return CanonicalBytes.copyOf(out.array());
    }

    public static M5MaterializationWorkerV1 decode(CanonicalBytes bytes) {
        if (bytes.length() != BYTES) {
            throw new IllegalArgumentException("worker grant length differs");
        }
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("worker grant magic/version differs");
        }
        var cell = digest(in);
        var scope = digest(in);
        var binding = new BindingIdentity(new TopicBindingId(digest(in)), digest(in), digest(in));
        long owner = in.getLong();
        long worker = in.getLong();
        long storage = in.getLong();
        var capability = new CapabilityBinding(in.getLong(), digest(in));
        var result = new M5MaterializationWorkerV1(
                new IdentityEnvelope(cell, scope, binding, owner, worker, storage, capability),
                digest(in),
                digest(in),
                digest(in),
                digest(in),
                digest(in),
                digest(in));
        if (!result.encode().equals(bytes)) {
            throw new IllegalArgumentException("worker grant bytes are not canonical");
        }
        return result;
    }

    private static void requireDigest(Sha256Digest digest) {
        if (Objects.requireNonNull(digest, "worker grant digest").isZero()) {
            throw new IllegalArgumentException("worker grant digest is zero");
        }
    }

    private static void digest(ByteBuffer out, Sha256Digest digest) {
        out.put(digest.bytes().toByteArray());
    }

    private static Sha256Digest digest(ByteBuffer in) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        in.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }
}
