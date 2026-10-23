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

package com.nereusstream.storage.object.retention;

import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.concurrent.CompletionStage;

/** A backend that serializes floor writers and the frontier at one existing authority cell. */
public interface M5SerializedLogicalTrimStoreV2 {
    CompletionStage<M5LogicalTrimCoordinatorV1.Result> advanceLogicalTrim(
            String frontierKey, RetentionFloorSnapshotV1 snapshot);
}
