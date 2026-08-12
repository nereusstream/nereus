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

package com.nereusstream.metadata.oxia.v2;

import java.util.Objects;

/** Construction inputs for one isolated V2 Oxia capability store. */
public record OxiaV2StoreConfiguration(String serviceAddress, String namespace, String authorityRoot) {
    public OxiaV2StoreConfiguration {
        serviceAddress = requireText(serviceAddress, "serviceAddress");
        namespace = requireText(namespace, "namespace");
        authorityRoot = requireText(authorityRoot, "authorityRoot");
        if (!authorityRoot.startsWith("/") || authorityRoot.endsWith("/") || authorityRoot.contains("//")) {
            throw new IllegalArgumentException("authorityRoot must be an absolute normalized Oxia prefix");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
