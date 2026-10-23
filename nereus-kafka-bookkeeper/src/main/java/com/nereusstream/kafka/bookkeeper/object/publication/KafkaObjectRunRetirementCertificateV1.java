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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1ConstantsV1;
import com.nereusstream.storage.object.control.CurrentWalRunPointer;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Package-private wire value, never a caller-supplied retirement capability. */
record KafkaObjectRunRetirementCertificateV1(
        WalRunRootRecord root,
        WalRunSealRecord seal,
        WalRunReference genesis,
        long prefixRuns,
        Optional<Sha256Digest> previousCertificate,
        CanonicalBytes protocolHead,
        List<CanonicalBytes> closedSnapshots) {
    static final int MAX_BYTES = 1024 * 1024;
    static final int MAX_MEMBERS = Nwkcp1ConstantsV1.FORMAT_MAX_ROWS;
    private static final int MAGIC = 0x4b525231;

    KafkaObjectRunRetirementCertificateV1 {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(seal, "seal");
        Objects.requireNonNull(genesis, "genesis");
        Objects.requireNonNull(previousCertificate, "previousCertificate");
        WalRunControlKeys.requireRootKey(genesis.rootKey(), genesis.shardId(), genesis.shardRunEpoch());
        requireProtocolClosure(root, seal, protocolHead);
        closedSnapshots = List.copyOf(closedSnapshots);
        if (!seal.root().rootSha256().equals(WalRunControlCodec.rootSha256(root))
                || seal.root().shardId() != root.shardId()
                || seal.root().shardRunEpoch() != root.shardRunEpoch()
                || genesis.shardId() != root.shardId()
                || prefixRuns <= 0
                || closedSnapshots.size() > MAX_MEMBERS
                || (seal.aggregateExtentCount() > 0 && closedSnapshots.isEmpty())
                || genesis.shardRunEpoch() > root.shardRunEpoch()
                || prefixRuns - 1 > root.shardRunEpoch() - genesis.shardRunEpoch()
                || root.predecessor().isPresent() != previousCertificate.isPresent()
                || previousCertificate.filter(Sha256Digest::isZero).isPresent()) {
            throw new IllegalArgumentException("invalid complete-prefix retirement identity/bounds");
        }
        if (root.predecessor().isEmpty() && (prefixRuns != 1 || !genesis.equals(seal.root()))) {
            throw new IllegalArgumentException("retirement prefix does not start at actual genesis");
        }
        if (root.predecessor().isPresent() && (prefixRuns < 2 || genesis.shardRunEpoch() >= root.shardRunEpoch())) {
            throw new IllegalArgumentException("retirement prefix extension is not increasing");
        }
        String prior = null;
        for (var snapshot : closedSnapshots) {
            String sha = Sha256Digest.hash(snapshot).toHex();
            if (prior != null && prior.compareTo(sha) >= 0) {
                throw new IllegalArgumentException("retirement member snapshots are duplicate or noncanonical");
            }
            prior = sha;
        }
    }

    CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(2);
                field(out, WalRunControlCodec.encodeRoot(root));
                field(out, WalRunControlCodec.encodeSeal(seal));
                field(out, WalRunControlCodec.encodePointer(new CurrentWalRunPointer(genesis)));
                out.writeLong(prefixRuns);
                out.writeBoolean(previousCertificate.isPresent());
                if (previousCertificate.isPresent()) {
                    out.write(previousCertificate.orElseThrow().bytes().toByteArray());
                }
                field(out, protocolHead);
                out.writeInt(closedSnapshots.size());
                for (var snapshot : closedSnapshots) {
                    field(out, snapshot);
                }
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("retirement certificate exceeds its control-value bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory retirement encoding failed", failure);
        }
    }

    static KafkaObjectRunRetirementCertificateV1 decode(CanonicalBytes raw) {
        if (raw.length() > MAX_BYTES) {
            throw new IllegalArgumentException("retirement certificate exceeds its control-value bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(raw.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 2) {
                throw new IllegalArgumentException("unsupported retirement certificate wire version");
            }
            var root = WalRunControlCodec.decodeRoot(field(in));
            var seal = WalRunControlCodec.decodeSeal(field(in));
            var genesis = WalRunControlCodec.decodePointer(field(in)).current();
            long count = in.readLong();
            Optional<Sha256Digest> previous = Optional.empty();
            if (in.readBoolean()) {
                byte[] sha = in.readNBytes(32);
                if (sha.length != 32) {
                    throw new IllegalArgumentException("truncated retirement predecessor digest");
                }
                previous = Optional.of(Sha256Digest.copyOf(sha));
            }
            var protocolHead = field(in);
            int members = in.readInt();
            if (members < 0 || members > MAX_MEMBERS) {
                throw new IllegalArgumentException("retirement member count exceeds its bound");
            }
            var snapshots = new ArrayList<CanonicalBytes>(members);
            for (int index = 0; index < members; index++) {
                snapshots.add(field(in));
            }
            var result = new KafkaObjectRunRetirementCertificateV1(
                    root, seal, genesis, count, previous, protocolHead, snapshots);
            if (in.read() != -1 || !result.encode().equals(raw)) {
                throw new IllegalArgumentException("noncanonical retirement certificate");
            }
            return result;
        } catch (IOException failure) {
            throw new IllegalArgumentException("truncated retirement certificate", failure);
        }
    }

    static void requireProtocolClosure(WalRunRootRecord root, WalRunSealRecord seal, CanonicalBytes raw) {
        if (com.nereusstream.storage.object.control.WalRunKafkaProtocolEmptyClosureV1.hasMagic(raw)) {
            var empty = com.nereusstream.storage.object.control.WalRunKafkaProtocolEmptyClosureV1.decode(raw);
            if (!empty.root().equals(root) || !empty.seal().equals(seal)) {
                throw new IllegalArgumentException("retirement protocol closure substituted its Root/Seal");
            }
        } else {
            var head = com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadCodecV1.decodeForRoot(
                    root, raw);
            if (head.state()
                            != com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadStateV1
                                    .TERMINAL
                    || head.coveredThroughVector().size() != 1) {
                throw new IllegalArgumentException(
                        "retirement requires the complete supported terminal protocol domain");
            }
        }
    }

    private static void field(DataOutputStream out, CanonicalBytes value) throws IOException {
        if (value.length() > MAX_BYTES) {
            throw new IllegalArgumentException("retirement field exceeds its bound");
        }
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes field(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > MAX_BYTES || size > in.available()) {
            throw new IllegalArgumentException("retirement field length is invalid");
        }
        return CanonicalBytes.copyOf(in.readNBytes(size));
    }
}
