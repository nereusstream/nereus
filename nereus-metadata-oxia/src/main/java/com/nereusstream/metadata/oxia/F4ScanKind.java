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

package com.nereusstream.metadata.oxia;

/** Process-local continuation-token scope for one bounded Phase 4 scan. */
public enum F4ScanKind {
    GENERATION_INDEX,
    MATERIALIZATION_TASK,
    MATERIALIZATION_CHECKPOINT,
    RETENTION_STATS,
    STREAM_REGISTRATION,
    PHYSICAL_ROOT,
    READER_LEASE,
    OBJECT_PROTECTION,
    GC_RETIREMENT_PROTECTION,
    GC_RETIREMENT_REMOVAL
}
