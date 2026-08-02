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

package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;

/**
 * Signals that an exact task source was valid when planned but has since been durably retired.
 *
 * <p>Callers must cancel and retire the stale task before resolving a fresh source set. They must
 * not retry the same immutable task or reopen the retired physical source.
 */
public final class MaterializationSourceRetiredException extends NereusException implements MaterializationFailure {

    public MaterializationSourceRetiredException(String message) {
        super(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    @Override
    public TaskFailureClass failureClass() {
        return TaskFailureClass.SOURCE_RETIRED;
    }
}
