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

package io.nereus.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class NereusExceptionTest {
    @Test
    void ordinaryFailureHasNoAppendOutcome() {
        NereusException exception = new NereusException(ErrorCode.OBJECT_READ_FAILED, true, "read failed");

        assertThat(exception.appendOutcome()).isEmpty();
    }

    @Test
    void appendFailureCarriesMachineReadableCommitCertainty() {
        NereusException exception = new NereusException(
                ErrorCode.TIMEOUT,
                true,
                "head CAS response timed out",
                AppendOutcome.MAY_HAVE_COMMITTED);

        assertThat(exception.appendOutcome()).contains(AppendOutcome.MAY_HAVE_COMMITTED);
    }

    @Test
    void failedAppendFuturePreservesOutcome() {
        CompletableFuture<Void> future = NereusException.failedAppendFuture(
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                AppendOutcome.KNOWN_COMMITTED,
                "derived index confirmation failed");

        assertThatThrownBy(future::join)
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .isInstanceOfSatisfying(NereusException.class, exception ->
                        assertThat(exception.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED));
    }
}
