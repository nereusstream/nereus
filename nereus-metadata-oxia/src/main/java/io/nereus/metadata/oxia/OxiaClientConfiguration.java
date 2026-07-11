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

package io.nereus.metadata.oxia;

import java.time.Duration;
import java.util.Objects;

/** Connection and bounded-scan settings for the production Oxia adapter. */
public record OxiaClientConfiguration(
        String serviceAddress,
        String namespace,
        Duration requestTimeout,
        Duration sessionTimeout,
        int maxCommitChainScan) {
    public OxiaClientConfiguration {
        serviceAddress = requireNonBlank(serviceAddress, "serviceAddress");
        namespace = requireNonBlank(namespace, "namespace");
        requestTimeout = requirePositive(requestTimeout, "requestTimeout");
        sessionTimeout = requirePositive(sessionTimeout, "sessionTimeout");
        if (maxCommitChainScan <= 0) {
            throw new IllegalArgumentException("maxCommitChainScan must be positive");
        }
    }

    public static OxiaClientConfiguration defaults(String serviceAddress) {
        return new OxiaClientConfiguration(
                serviceAddress,
                "default",
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                10_000);
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
