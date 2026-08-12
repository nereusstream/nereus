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

package com.nereusstream.metadata.oxia.v2.mutation;

import static org.assertj.core.api.Assertions.assertThat;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class MutationFailureClassifierTest {
    private final MutationFailureClassifier classifier = new MutationFailureClassifier();

    @Test
    void nestedAlreadyExistsIsAConditionFailure() {
        Throwable failure = new CompletionException(new ExecutionException(new KeyAlreadyExistsException("key")));

        assertThat(classifier.classify(failure)).isEqualTo(MutationFailureClassifier.Kind.CONDITION_FAILED);
    }

    @Test
    void nestedUnexpectedVersionIsAConditionFailure() {
        assertThat(classifier.classify(new CompletionException(new UnexpectedVersionIdException("key", 3))))
                .isEqualTo(MutationFailureClassifier.Kind.CONDITION_FAILED);
    }

    @Test
    void timeoutTransportAndCancellationLikeFailuresAreResponseUnknown() {
        assertThat(classifier.classify(new IllegalStateException("transport lost")))
                .isEqualTo(MutationFailureClassifier.Kind.RESPONSE_UNKNOWN);
    }
}
