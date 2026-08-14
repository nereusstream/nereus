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

package com.nereusstream.kafka.bookkeeper.nbke2;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.List;
import java.util.Optional;

final class Nbke2TestFrames {
    static final long LEDGER_ID = 41;

    private Nbke2TestFrames() {}

    static Nbke2RunBindingV1 binding() {
        return new Nbke2RunBindingV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName("orders")),
                7,
                new StorageEpochId(digest(3)),
                11,
                5,
                new CellProviderScopeId(digest(4)),
                new StorageRunId(new Id128(0, 6)));
    }

    static Nbke2RunHeaderV1 runHeader() {
        return new Nbke2RunHeaderV1(binding(), 100, 1, digest(7));
    }

    static Nbke2DataV1 data() {
        CanonicalBytes raw = CanonicalBytes.copyOf(new byte[] {0x10, 0x20, 0x30, 0x40, 0x50});
        return new Nbke2DataV1(
                binding(),
                100,
                2,
                0,
                1,
                new Id128(0, 8),
                new Id128(0, 9),
                Optional.of(new Nbke2AppendGroupDescriptorV1(100, 103, 1, 1, Sha256Digest.hash(raw))),
                raw);
    }

    static Nbke2RangeIndexBlockV1 rangeIndexBlock() {
        return new Nbke2RangeIndexBlockV1(
                binding(),
                100,
                1,
                105,
                1,
                2,
                -1,
                4,
                List.of(new Nbke2BatchLocatorV1(0, 3, 0, 0, 0, 5, 1), new Nbke2BatchLocatorV1(3, 2, 1, 1, 0, 7, 1)));
    }

    static Nbke2ProtocolCheckpointV1 protocolCheckpoint() {
        return new Nbke2ProtocolCheckpointV1(
                binding(),
                105,
                104,
                103,
                105,
                CanonicalBytes.copyOf(new byte[] {1, 2}),
                CanonicalBytes.copyOf(new byte[] {3, 4, 5}),
                CanonicalBytes.copyOf(new byte[] {6}));
    }

    static Nbke2RunFooterV1 runFooter() {
        return new Nbke2RunFooterV1(binding(), 105, 6, 3, 4, 12, List.of(new Nbke2IndexDirectoryEntryV1(3, 100, 105)));
    }

    static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
