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

package com.nereusstream.storage.api.lifecycle;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A physical backend's own permanent metadata-route assignment, read through the actual admitted backend connection.
 * Implementations verify native namespace incarnation, prohibit reassignment, and fence old creation admission when
 * installing a route. Endpoint strings, caller booleans and a locally reconstructed binding do not implement this port.
 */
public interface NativePhysicalNamespaceAuthorityV2 {
    PhysicalResourceIdV2.Namespace namespace();

    CompletionStage<Optional<PhysicalNamespaceAuthorityBindingV2>> readBinding();

    CompletionStage<PhysicalNamespaceAuthorityBindingV2> bind(MetadataNamespaceIdentityV2 metadataNamespace);
}
