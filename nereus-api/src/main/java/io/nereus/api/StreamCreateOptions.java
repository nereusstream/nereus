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

import java.util.Map;
import java.util.Objects;

/**
 * Options used when creating or looking up a stream.
 *
 * <p>For an existing stream these values do not mutate its stored profile or attributes; profile migration is a
 * separate protocol.
 */
public record StreamCreateOptions(
        StorageProfile profile,
        Map<String, String> attributes) {
    public StreamCreateOptions {
        Objects.requireNonNull(profile, "profile");
        attributes = MetadataCanonicalizer.canonicalStringMap(
                attributes,
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "attributes");
    }
}
