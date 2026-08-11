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

package com.nereusstream.domain.codec;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Domain-separated NTB1 and NSE1 deterministic identity derivations. */
public final class DeterministicTopicIdsV1 {
    private static final byte[] BINDING_MAGIC = "NTB1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] EPOCH_MAGIC = "NSE1".getBytes(StandardCharsets.US_ASCII);

    private DeterministicTopicIdsV1() {}

    public static TopicBindingId deriveBindingId(
            ProtocolCellIdentity cellIdentity, TopicIncarnationIdentity incarnationIdentity) {
        return new TopicBindingId(Sha256Digest.hash(bindingPreimage(cellIdentity, incarnationIdentity)));
    }

    public static CanonicalBytes bindingPreimage(
            ProtocolCellIdentity cellIdentity, TopicIncarnationIdentity incarnationIdentity) {
        Objects.requireNonNull(cellIdentity, "cellIdentity");
        Objects.requireNonNull(incarnationIdentity, "incarnationIdentity");
        if (cellIdentity.protocolKind() != incarnationIdentity.protocolKind()) {
            throw new IllegalArgumentException("Cell and incarnation protocol kinds differ");
        }
        CanonicalBytes cellBytes = ProtocolCellIdentityCodecV1.encode(cellIdentity);
        CanonicalBytes incarnationBytes = TopicIncarnationIdentityCodecV1.encode(incarnationIdentity);
        int length = WireCodecSupport.checkedSize(
                BINDING_MAGIC.length, Integer.BYTES, cellBytes.length(), Integer.BYTES, incarnationBytes.length());
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(BINDING_MAGIC);
        WireCodecSupport.putLength(buffer, cellBytes.length());
        buffer.put(cellBytes.toByteArray());
        WireCodecSupport.putLength(buffer, incarnationBytes.length());
        buffer.put(incarnationBytes.toByteArray());
        return WireCodecSupport.finish(buffer);
    }

    public static StorageEpochId deriveStorageEpochId(TopicBindingId bindingId, long epochOrdinal) {
        return new StorageEpochId(Sha256Digest.hash(storageEpochPreimage(bindingId, epochOrdinal)));
    }

    public static CanonicalBytes storageEpochPreimage(TopicBindingId bindingId, long epochOrdinal) {
        Objects.requireNonNull(bindingId, "bindingId");
        if (epochOrdinal != 0) {
            throw new IllegalArgumentException("M1 foundation accepts only epoch ordinal zero");
        }
        ByteBuffer buffer = ByteBuffer.allocate(EPOCH_MAGIC.length + Sha256Digest.LENGTH + Long.BYTES);
        buffer.put(EPOCH_MAGIC);
        buffer.put(bindingId.digest().bytes().toByteArray());
        buffer.putLong(epochOrdinal);
        return WireCodecSupport.finish(buffer);
    }
}
