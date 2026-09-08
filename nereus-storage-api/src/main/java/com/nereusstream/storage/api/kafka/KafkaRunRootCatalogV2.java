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

package com.nereusstream.storage.api.kafka;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Selected native roots on one assigned namespace/protocol route; this read is not physical access admission. */
public interface KafkaRunRootCatalogV2 {
    CompletionStage<Optional<KafkaRunRootRecordV2>> readSelectedRoot(String nativeKey);
}
