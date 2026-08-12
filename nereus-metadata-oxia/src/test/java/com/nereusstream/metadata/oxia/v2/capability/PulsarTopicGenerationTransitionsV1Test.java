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

package com.nereusstream.metadata.oxia.v2.capability;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.metadata.oxia.v2.codec.Nps1SelectorAuthorityCodec;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import org.junit.jupiter.api.Test;

class PulsarTopicGenerationTransitionsV1Test {
    private static final PulsarPersistenceName NAME = PulsarPersistenceName.fromString("tenant/ns/persistent/topic");
    private static final TopicBindingId FIRST_ID = new TopicBindingId(Sha256Digest.hash(bytes(1)));
    private static final TopicBindingId SECOND_ID = new TopicBindingId(Sha256Digest.hash(bytes(2)));
    private static final Sha256Digest FIRST_DIGEST = Sha256Digest.hash(bytes(3));
    private static final Sha256Digest SECOND_DIGEST = Sha256Digest.hash(bytes(4));

    @Test
    void acceptsFirstReservationAndThreeSameGenerationEdges() {
        var reserved = value(1, PulsarTopicGenerationSelectorStateV1.RESERVED, FIRST_ID, FIRST_DIGEST);
        var active = value(1, PulsarTopicGenerationSelectorStateV1.ACTIVE, FIRST_ID, FIRST_DIGEST);
        var deleting = value(1, PulsarTopicGenerationSelectorStateV1.DELETING, FIRST_ID, FIRST_DIGEST);
        var deleted = value(1, PulsarTopicGenerationSelectorStateV1.DELETED, FIRST_ID, FIRST_DIGEST);

        assertThatCode(() -> PulsarTopicGenerationTransitionsV1.requireFirstCreate(reserved))
                .doesNotThrowAnyException();
        assertThatCode(() -> PulsarTopicGenerationTransitionsV1.requireCas(reserved, active))
                .doesNotThrowAnyException();
        assertThatCode(() -> PulsarTopicGenerationTransitionsV1.requireCas(active, deleting))
                .doesNotThrowAnyException();
        assertThatCode(() -> PulsarTopicGenerationTransitionsV1.requireCas(deleting, deleted))
                .doesNotThrowAnyException();
    }

    @Test
    void acceptsRecreationWithNewExactIdentity() {
        var deleted = value(1, PulsarTopicGenerationSelectorStateV1.DELETED, FIRST_ID, FIRST_DIGEST);
        var reserved = value(2, PulsarTopicGenerationSelectorStateV1.RESERVED, SECOND_ID, SECOND_DIGEST);
        assertThatCode(() -> PulsarTopicGenerationTransitionsV1.requireCas(deleted, reserved))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsWrongFirstCreateIllegalEdgesAndIdentityDrift() {
        var reserved = value(1, PulsarTopicGenerationSelectorStateV1.RESERVED, FIRST_ID, FIRST_DIGEST);
        var active = value(1, PulsarTopicGenerationSelectorStateV1.ACTIVE, FIRST_ID, FIRST_DIGEST);
        assertThatThrownBy(() -> PulsarTopicGenerationTransitionsV1.requireFirstCreate(active))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarTopicGenerationTransitionsV1.requireFirstCreate(
                        value(2, PulsarTopicGenerationSelectorStateV1.RESERVED, FIRST_ID, FIRST_DIGEST)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarTopicGenerationTransitionsV1.requireCas(active, reserved))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarTopicGenerationTransitionsV1.requireCas(
                        reserved, value(1, PulsarTopicGenerationSelectorStateV1.ACTIVE, SECOND_ID, FIRST_DIGEST)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsGenerationSkipAndOverflow() {
        var deleted = value(1, PulsarTopicGenerationSelectorStateV1.DELETED, FIRST_ID, FIRST_DIGEST);
        assertThatThrownBy(() -> PulsarTopicGenerationTransitionsV1.requireCas(
                        deleted, value(3, PulsarTopicGenerationSelectorStateV1.RESERVED, SECOND_ID, SECOND_DIGEST)))
                .isInstanceOf(IllegalArgumentException.class);

        var terminal = value(Long.MAX_VALUE, PulsarTopicGenerationSelectorStateV1.DELETED, FIRST_ID, FIRST_DIGEST);
        assertThatThrownBy(() -> PulsarTopicGenerationTransitionsV1.requireCas(
                        terminal, value(1, PulsarTopicGenerationSelectorStateV1.RESERVED, SECOND_ID, SECOND_DIGEST)))
                .isInstanceOf(ArithmeticException.class);
    }

    private static PulsarTopicGenerationSelectorValueV1 value(
            long generation,
            PulsarTopicGenerationSelectorStateV1 state,
            TopicBindingId bindingId,
            Sha256Digest digest) {
        return Nps1SelectorAuthorityCodec.createValue(
                NAME, new PulsarBindingGeneration(generation), state, bindingId, digest);
    }

    private static com.nereusstream.domain.bytes.CanonicalBytes bytes(int value) {
        return com.nereusstream.domain.bytes.CanonicalBytes.copyOf(new byte[] {(byte) value});
    }
}
