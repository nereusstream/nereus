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

/** Durability/visibility boundary requested by append. */
public enum DurabilityLevel {
    /**
     * The primary WAL append is durable before the append future succeeds.
     *
     * <p>This is the fast-ack boundary used by AutoMQ-like profiles. Implementations still have to return
     * stable stream offsets and protocol projection references; they just must not wait for read-optimized
     * object materialization before acknowledging the caller.
     */
    WAL_DURABLE,

    /** The primary WAL append is durable and the Oxia read index is committed before the append succeeds. */
    WAL_DURABLE_AND_INDEX_COMMITTED
}
