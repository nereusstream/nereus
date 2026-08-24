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

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Runtime queue descriptor; Root SHA is checked at admission and omitted from the canonical physical row. */
public record ProviderResolvedExtentDescriptor(
        Sha256Digest rootSha256, ProviderResolvedExtentRowV1 row, long providerResolvedAtMillis) {
    public ProviderResolvedExtentDescriptor {
        Objects.requireNonNull(rootSha256, "rootSha256");
        Objects.requireNonNull(row, "row");
        if (rootSha256.isZero() || providerResolvedAtMillis < 0) {
            throw new IllegalArgumentException("provider-resolved descriptor identity is invalid");
        }
    }
}
