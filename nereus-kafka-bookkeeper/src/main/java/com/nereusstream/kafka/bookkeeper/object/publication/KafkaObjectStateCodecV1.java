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
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectActiveTailStateV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Exact canonical bytes used for Object active-tail repository references and reservation charge. */
public final class KafkaObjectStateCodecV1 {
    private static final String LOCATOR_TAG = "M3-KAFKA-OBJECT-LOCATOR-V1";
    private static final String ACTIVE_TAIL_TAG = "M3-KAFKA-OBJECT-ACTIVE-TAIL-V1";
    private static final int LOCATOR_FIXED_FIELDS_BYTES = 200;
    private static final int LOCATOR_BYTES =
            Integer.BYTES + LOCATOR_TAG.getBytes(StandardCharsets.UTF_8).length + LOCATOR_FIXED_FIELDS_BYTES;

    private KafkaObjectStateCodecV1() {}

    public static int exactLocatorBytes() {
        return LOCATOR_BYTES;
    }

    public static CanonicalBytes locator(KafkaObjectExtentLocatorV1 locator) {
        CanonicalBytes encoded = encode(out -> writeLocator(out, locator));
        if (encoded.length() != LOCATOR_BYTES) {
            throw new IllegalStateException("Kafka Object locator fixed wire length drifted");
        }
        return encoded;
    }

    public static CanonicalBytes activeTail(KafkaObjectActiveTailStateV1 state) {
        return encode(out -> {
            tag(out, ACTIVE_TAIL_TAG);
            binding(out, state.binding());
            out.writeLong(state.startOffset());
            out.writeLong(state.endOffsetExclusive());
            out.writeInt(state.locators().size());
            for (KafkaObjectExtentLocatorV1 locator : state.locators()) {
                CanonicalBytes bytes = locator(locator);
                out.writeInt(bytes.length());
                out.write(bytes.toByteArray());
            }
        });
    }

    private static void writeLocator(DataOutputStream out, KafkaObjectExtentLocatorV1 locator) throws IOException {
        tag(out, LOCATOR_TAG);
        binding(out, locator.binding());
        out.writeLong(locator.startOffset());
        out.writeLong(locator.endOffsetExclusive());
        KafkaObjectExtentIdentityV1 extent = locator.extent();
        out.write(extent.walRunRootSha().bytes().toByteArray());
        out.writeInt(extent.laneId());
        out.writeLong(extent.laneSequence());
        out.writeLong(extent.directoryPrefixEnd());
        out.writeLong(extent.bodyLength());
        out.write(extent.bodySha().bytes().toByteArray());
        out.writeInt(locator.firstDirectoryRow());
        out.writeInt(locator.directoryRowCount());
    }

    private static void binding(DataOutputStream out, KafkaObjectBindingKeyV1 binding) throws IOException {
        out.write(binding.bindingId().digest().bytes().toByteArray());
        out.write(binding.topicId().value().bytes().toByteArray());
        out.writeInt(binding.partitionId());
        out.write(binding.storageEpochId().digest().bytes().toByteArray());
    }

    private static void tag(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writer.write(out);
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory Kafka Object state encoding failed", failure);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }
}
