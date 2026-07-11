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

package com.nereusstream.api;

/** Public Phase 1 API value limits shared by metadata and object modules. */
public final class ApiLimits {
    public static final int MAX_STREAM_ATTRIBUTES_ENCODED_BYTES = 16 * 1024;
    public static final int MAX_ENTRY_ATTRIBUTES_ENCODED_BYTES = 16 * 1024;
    public static final int MAX_SCHEMA_REFS_ENCODED_BYTES = 16 * 1024;

    private ApiLimits() {
    }
}
