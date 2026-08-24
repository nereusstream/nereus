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

package com.nereusstream.storage.object.provider;

/** Runtime Provider outcome table. UNKNOWN is an explicit non-terminal state, never absence. */
public enum ProviderObjectOutcome {
    APPLIED_EXACT,
    EXISTING_EXACT,
    DEFINITIVELY_NOT_APPLIED,
    DEFINITIVE_CONFLICT,
    OUTCOME_UNKNOWN
}
