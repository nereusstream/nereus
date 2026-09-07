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

package com.nereusstream.storage.object.gc;

import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Native durable owner/capability verification, independent of metadata fact existence.
 *
 * <p>An implementation must decode native authority, prove the current owner and admitted resource capability,
 * and prove a displaced owner durably fenced. Lease timeout, local callbacks and caller-supplied hashes are not
 * such proof. Revocation must fence local work before another owner is admitted; relevant admissions remain
 * serialized by the permanent resource authority. Missing protocol adapters are unsupported.
 */
public interface DeleteObservationAuthorityVerifierV2 {
    CompletionStage<Void> requireCurrent(PhysicalResourceIdV2 resource, DeleteObservationContextV2 current);

    CompletionStage<Void> requirePredecessorFenced(
            PhysicalResourceIdV2 resource, DeleteObservationContextV2 previous, DeleteObservationContextV2 successor);

    static DeleteObservationAuthorityVerifierV2 unsupported() {
        return new DeleteObservationAuthorityVerifierV2() {
            @Override
            public CompletionStage<Void> requireCurrent(
                    PhysicalResourceIdV2 resource, DeleteObservationContextV2 current) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException("native deletion observation authority is not installed"));
            }

            @Override
            public CompletionStage<Void> requirePredecessorFenced(
                    PhysicalResourceIdV2 resource,
                    DeleteObservationContextV2 previous,
                    DeleteObservationContextV2 successor) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException("native deletion owner fencing is not installed"));
            }
        };
    }
}
