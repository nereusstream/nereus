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

package com.nereusstream.storage.object.gc;

import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.util.concurrent.CompletionStage;

/**
 * A physical backend's uniquely bound native metadata route with an existing active authority quota reservation.
 * Implementations reread both native namespace identities and the durable resource reservation/authority. This
 * port does not supply protocol eligibility, authority-time grace, dispatch capacity or native-metadata capacity.
 */
public interface BoundPhysicalDeleteAuthorityRouteV2 extends ExactMetadataTransactionStoreV1 {
    CompletionStage<PhysicalNamespaceAuthorityBindingV2> requireActiveResource(PhysicalResourceIdV2 resource);
}
