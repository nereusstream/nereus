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

package com.nereusstream.domain.registry;

/** Stable Registry-v1 validation failures. */
public enum RegistryRejectionCodeV1 {
    REGISTRY_IDENTITY_INVALID,
    REGISTRY_WRITER_COUNT_EXCEEDED,
    REGISTRY_CANONICAL_BYTES_EXCEEDED,
    REGISTRY_ASSIGNMENT_COUNT_EXCEEDED,
    REGISTRY_ASSIGNMENT_ROW_BYTES_EXCEEDED,
    REGISTRY_OMITTED_AUTHORIZED_WRITER,
    REGISTRY_UNAUTHORIZED_WRITER,
    REGISTRY_WRITER_LIFECYCLE_VIOLATION,
    REGISTRY_ASSIGNMENT_INVALID,
    REGISTRY_EPOCH_INVALID,
    REGISTRY_NON_CANONICAL
}
