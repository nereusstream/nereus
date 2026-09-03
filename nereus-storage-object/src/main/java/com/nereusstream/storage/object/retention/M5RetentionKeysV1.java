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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.util.Locale;
import java.util.Objects;

/** Deterministic bounded keys for M5-C retention and M4 batch metadata normalization. */
public final class M5RetentionKeysV1 {
    private final String m4Prefix;
    private final String m5Prefix;

    public M5RetentionKeysV1(int shardId, BindingIdentity binding) {
        Objects.requireNonNull(binding, "binding");
        if (shardId < 0) {
            throw new IllegalArgumentException("shard ID must be non-negative");
        }
        String shard = String.format(Locale.ROOT, "%010d", shardId);
        String bindingHex = binding.bindingId().digest().toHex();
        m4Prefix = "v2/object-wal/shards/" + shard + "/read-m4/" + bindingHex;
        m5Prefix = "v2/object-wal/shards/" + shard + "/read-m5/" + bindingHex + "/retention";
    }

    public String trimFrontier() {
        return m5Prefix + "/trim-frontier";
    }

    public String floorSnapshot(Sha256Digest snapshotRootSha256) {
        return m5Prefix + "/floor-snapshots/" + digest(snapshotRootSha256);
    }

    public String referenceFreeProof(Sha256Digest proofSha256) {
        return m5Prefix + "/reference-free-proofs/" + digest(proofSha256);
    }

    public String retirementBatch(Sha256Digest batchIdSha256) {
        return m4Prefix + "/retirement-batches/" + digest(batchIdSha256);
    }

    public String transactionPartitionKey() {
        return m4Prefix;
    }

    private static String digest(Sha256Digest digest) {
        M5RetentionRecordsV1.requireDigest(digest, "key digest");
        return digest.toHex();
    }
}
