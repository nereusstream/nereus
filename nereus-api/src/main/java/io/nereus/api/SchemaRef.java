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

import java.util.Objects;

/** Slice-level schema reference carried through append, WAL, offset index, and read. */
public record SchemaRef(
        String namespace,
        String id,
        long version) {
    public SchemaRef {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(id, "id");
        if (namespace.isBlank() || id.isBlank()) {
            throw new IllegalArgumentException("schema namespace and id cannot be blank");
        }
        if (version < 0) {
            throw new IllegalArgumentException("schema version must be non-negative");
        }
    }
}
