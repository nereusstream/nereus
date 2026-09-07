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
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/** One immutable selection decision, first anchored atomically with the existing Binding selector CAS. */
public record M5TaskSelectionDecisionV2(
        BindingIdentity binding,
        Sha256Digest taskId,
        Outcome outcome,
        Optional<Sha256Digest> selectedOutput,
        Sha256Digest predecessorSelector,
        Sha256Digest successorSelector) {
    private static final int MAGIC = 0x4d355444; // M5TD
    public static final int MAX_BYTES = 232;
    private static final int BASE_BYTES = 200;

    public enum Outcome {
        SELECTED,
        SELECTION_CANCELLED
    }

    public M5TaskSelectionDecisionV2 {
        Objects.requireNonNull(binding, "binding");
        requireDigest(taskId);
        Objects.requireNonNull(outcome, "outcome");
        selectedOutput = Objects.requireNonNull(selectedOutput, "selectedOutput");
        selectedOutput.ifPresent(M5TaskSelectionDecisionV2::requireDigest);
        requireDigest(predecessorSelector);
        requireDigest(successorSelector);
        if ((outcome == Outcome.SELECTED) != selectedOutput.isPresent()
                || outcome == Outcome.SELECTION_CANCELLED && !predecessorSelector.equals(successorSelector)) {
            throw new IllegalArgumentException("task selection outcome and selector identities disagree");
        }
    }

    public static M5TaskSelectionDecisionV2 of(
            Sha256Digest taskId, Outcome outcome, BindingReadSelector predecessor, BindingReadSelector successor) {
        if (!predecessor.binding().equals(successor.binding())) {
            throw new IllegalArgumentException("task selection changes Binding");
        }
        return new M5TaskSelectionDecisionV2(
                predecessor.binding(),
                taskId,
                outcome,
                outcome == Outcome.SELECTED ? Optional.of(successor.selectedViewSha256()) : Optional.empty(),
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(predecessor)),
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(successor)));
    }

    public String key() {
        return key(binding, taskId);
    }

    public static String key(BindingIdentity binding, Sha256Digest taskId) {
        requireDigest(taskId);
        return "v2/m5-task-select/" + binding.bindingId().digest().toHex() + "/"
                + binding.incarnationSha256().toHex() + "/"
                + binding.storageEpochSha256().toHex()
                + "/decisions/" + taskId.toHex();
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(BASE_BYTES + (selectedOutput.isPresent() ? 32 : 0));
        out.putInt(MAGIC)
                .putShort((short) 2)
                .put((byte) 1) // Closed protocol family: Kafka BK compaction task.
                .put((byte) (outcome == Outcome.SELECTED ? 1 : 2));
        out.put(binding.bindingId().digest().bytes().toByteArray());
        out.put(binding.incarnationSha256().bytes().toByteArray());
        out.put(binding.storageEpochSha256().bytes().toByteArray());
        out.put(taskId.bytes().toByteArray());
        selectedOutput.ifPresent(value -> out.put(value.bytes().toByteArray()));
        out.put(predecessorSelector.bytes().toByteArray())
                .put(successorSelector.bytes().toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    public static M5TaskSelectionDecisionV2 decode(CanonicalBytes bytes) {
        if (bytes.length() != BASE_BYTES && bytes.length() != MAX_BYTES) {
            throw new IllegalArgumentException("task selection decision byte length differs");
        }
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.getInt() != MAGIC || in.getShort() != 2 || in.get() != 1) {
            throw new IllegalArgumentException("task selection decision magic, version or family differs");
        }
        byte code = in.get();
        if (code != 1 && code != 2 || bytes.length() != (code == 1 ? MAX_BYTES : BASE_BYTES)) {
            throw new IllegalArgumentException("task selection decision outcome/length differs");
        }
        var binding = new BindingIdentity(new TopicBindingId(digest(in)), digest(in), digest(in));
        var task = digest(in);
        var output = code == 1 ? Optional.of(digest(in)) : Optional.<Sha256Digest>empty();
        var value = new M5TaskSelectionDecisionV2(
                binding,
                task,
                code == 1 ? Outcome.SELECTED : Outcome.SELECTION_CANCELLED,
                output,
                digest(in),
                digest(in));
        if (!value.encode().equals(bytes)) {
            throw new IllegalArgumentException("task selection decision is not canonical");
        }
        return value;
    }

    private static Sha256Digest digest(ByteBuffer input) {
        byte[] bytes = new byte[32];
        input.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private static void requireDigest(Sha256Digest digest) {
        if (Objects.requireNonNull(digest, "digest").isZero()) {
            throw new IllegalArgumentException("task selection decision has a zero identity");
        }
    }
}
