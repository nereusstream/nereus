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
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/**
 * Source initialization at the selector key, before a selected view exists.
 *
 * <p>This is an initialization fence, not closed-writer enrollment or evidence of reference absence.
 * Its exact bytes survive selector activation; existing selectors cannot acquire a bootstrap retroactively.
 * A bootstrap alone does not prove that
 * historical writers or external references were closed.
 */
public record M5BindingBootstrapV1(
        BindingIdentity binding,
        CapabilityBinding capability,
        Sha256Digest initialSourceScopeSha256,
        Optional<M5KafkaBindingPolicyV1> kafkaPolicy) {
    private static final int MAGIC = 0x4d354253; // M5BS
    private static final int VERSION = 1;
    private static final int LENGTH = 2 * Integer.BYTES + Long.BYTES + 6 * Sha256Digest.LENGTH;

    public M5BindingBootstrapV1(
            BindingIdentity binding, CapabilityBinding capability, Sha256Digest initialSourceScopeSha256) {
        this(binding, capability, initialSourceScopeSha256, Optional.empty());
    }

    public M5BindingBootstrapV1 {
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(capability, "capability");
        M5RetentionRecordsV1.requireDigest(initialSourceScopeSha256, "initialSourceScopeSha256");
        kafkaPolicy = Objects.requireNonNull(kafkaPolicy, "kafkaPolicy");
        if (kafkaPolicy
                .filter(value -> value.generation() != 1 || !value.auditHolds().isEmpty())
                .isPresent()) {
            throw new IllegalArgumentException("Kafka bootstrap requires its first explicitly empty policy generation");
        }
    }

    public CanonicalBytes encode() {
        var policy = kafkaPolicy.map(M5KafkaBindingPolicyV1::encode);
        int length = policy.map(value -> Math.addExact(LENGTH + Integer.BYTES, value.length()))
                .orElse(LENGTH);
        var buffer = ByteBuffer.allocate(length);
        buffer.putInt(MAGIC).putInt(policy.isPresent() ? 2 : VERSION);
        put(buffer, binding.bindingId().digest());
        put(buffer, binding.incarnationSha256());
        put(buffer, binding.storageEpochSha256());
        buffer.putLong(capability.generation());
        put(buffer, capability.evidenceSha256());
        put(buffer, initialSourceScopeSha256);
        policy.ifPresent(value -> buffer.putInt(value.length()).put(value.toByteArray()));
        var body = CanonicalBytes.copyOf(java.util.Arrays.copyOf(buffer.array(), length - Sha256Digest.LENGTH));
        put(buffer, Sha256Digest.hash(body));
        return CanonicalBytes.copyOf(buffer.array());
    }

    public static boolean isBootstrap(CanonicalBytes encoded) {
        var bytes = encoded.toByteArray();
        return bytes.length >= Integer.BYTES && ByteBuffer.wrap(bytes).getInt() == MAGIC;
    }

    public static M5BindingBootstrapV1 decode(CanonicalBytes encoded) {
        if (encoded.length() < LENGTH || encoded.length() > LENGTH + Integer.BYTES + M5KafkaBindingPolicyV1.MAX_BYTES) {
            throw new IllegalArgumentException("Binding bootstrap length differs");
        }
        var buffer = ByteBuffer.wrap(encoded.toByteArray());
        if (buffer.getInt() != MAGIC) {
            throw new IllegalArgumentException("Binding bootstrap preamble differs");
        }
        int version = buffer.getInt();
        if (version != 1 && version != 2 || version == 1 && encoded.length() != LENGTH) {
            throw new IllegalArgumentException("Binding bootstrap version or length differs");
        }
        var binding = new BindingIdentity(new TopicBindingId(read(buffer)), read(buffer), read(buffer));
        var capability = new CapabilityBinding(buffer.getLong(), read(buffer));
        var source = read(buffer);
        Optional<M5KafkaBindingPolicyV1> policy = Optional.empty();
        if (version == 2) {
            if (buffer.remaining() < Integer.BYTES + Sha256Digest.LENGTH) {
                throw new IllegalArgumentException("Binding bootstrap policy length is absent");
            }
            int count = buffer.getInt();
            if (count <= 0
                    || count > M5KafkaBindingPolicyV1.MAX_BYTES
                    || count != buffer.remaining() - Sha256Digest.LENGTH) {
                throw new IllegalArgumentException("Binding bootstrap policy length differs");
            }
            var bytes = new byte[count];
            buffer.get(bytes);
            policy = Optional.of(M5KafkaBindingPolicyV1.decode(CanonicalBytes.copyOf(bytes)));
        }
        var value = new M5BindingBootstrapV1(binding, capability, source, policy);
        if (!value.encode().equals(encoded)) {
            throw new IllegalArgumentException("Binding bootstrap checksum or canonical bytes differ");
        }
        return value;
    }

    private static void put(ByteBuffer buffer, Sha256Digest digest) {
        buffer.put(digest.bytes().toByteArray());
    }

    private static Sha256Digest read(ByteBuffer buffer) {
        var bytes = new byte[Sha256Digest.LENGTH];
        buffer.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }
}
