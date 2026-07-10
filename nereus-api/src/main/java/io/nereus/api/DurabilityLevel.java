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

/** Durability and derived-read-index boundary requested by append. */
public enum DurabilityLevel {
    /**
     * The primary WAL append is durable and the logical append has a stable Oxia commit before the append future
     * succeeds. The generation-zero read index may still require repair from the reachable commit record.
     *
     * <p>This is the target fast-publication boundary for AutoMQ-like and WAL-only profiles. It never permits a
     * WAL-only acknowledgment with broker-local offsets. Implementations still have to return stable stream
     * offsets, a recoverable primary read target, and protocol projection references; they just need not wait for
     * derived-index confirmation or read-optimized object materialization.
     */
    WAL_DURABLE,

    /**
     * The primary WAL append and stable logical commit are durable, and the generation-zero Oxia read/replay
     * indexes are confirmed before the append succeeds.
     */
    WAL_DURABLE_AND_INDEX_COMMITTED
}
