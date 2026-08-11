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
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** NPN1 digest and relative Pulsar selector/aggregate authority-leaf grammar. */
public final class PulsarAuthorityLeafCodecV1 {
    private static final byte[] MAGIC = "NPN1".getBytes(StandardCharsets.US_ASCII);
    private static final int GENERATION_WIDTH = 19;

    private PulsarAuthorityLeafCodecV1() {}

    public static Sha256Digest nameDigest(PulsarPersistenceName persistenceName) {
        return Sha256Digest.hash(nameDigestPreimage(persistenceName));
    }

    public static CanonicalBytes nameDigestPreimage(PulsarPersistenceName persistenceName) {
        Objects.requireNonNull(persistenceName, "persistenceName");
        CanonicalBytes name = persistenceName.value().bytes();
        int length = WireCodecSupport.checkedSize(MAGIC.length, Integer.BYTES, name.length());
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(MAGIC);
        WireCodecSupport.putLength(buffer, name.length());
        buffer.put(name.toByteArray());
        return WireCodecSupport.finish(buffer);
    }

    public static String selectorLeaf(PulsarPersistenceName persistenceName) {
        return nameDigest(persistenceName).toHex();
    }

    public static String aggregateLeaf(PulsarPersistenceName persistenceName, PulsarBindingGeneration generation) {
        Objects.requireNonNull(generation, "generation");
        return selectorLeaf(persistenceName) + "/" + generation19(generation);
    }

    public static String generation19(PulsarBindingGeneration generation) {
        Objects.requireNonNull(generation, "generation");
        String digits = Long.toString(generation.value());
        return "0".repeat(GENERATION_WIDTH - digits.length()) + digits;
    }

    public static void requireSelectorLeaf(String actualLeaf, PulsarPersistenceName persistenceName) {
        Objects.requireNonNull(actualLeaf, "actualLeaf");
        if (!actualLeaf.equals(selectorLeaf(persistenceName))) {
            throw new IllegalArgumentException("selector leaf does not match its canonical persistence name");
        }
    }

    public static void requireAggregateLeaf(
            String actualLeaf, PulsarPersistenceName persistenceName, PulsarBindingGeneration generation) {
        Objects.requireNonNull(actualLeaf, "actualLeaf");
        if (!actualLeaf.equals(aggregateLeaf(persistenceName, generation))) {
            throw new IllegalArgumentException("aggregate leaf does not match its canonical name and generation");
        }
    }
}
