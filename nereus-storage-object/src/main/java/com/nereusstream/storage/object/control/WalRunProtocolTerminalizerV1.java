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

package com.nereusstream.storage.object.control;

import java.util.concurrent.CompletionStage;

/** Protocol adapter seam: only an issued physical closure proof may terminalize a protocol checkpoint Head. */
@FunctionalInterface
public interface WalRunProtocolTerminalizerV1 {
    CompletionStage<TerminalProtocolCheckpointBindingV1> terminalize(WalRunTerminalClosureProofV1 closureProof);
}
