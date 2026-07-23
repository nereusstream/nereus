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

package com.nereusstream.core.append;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendPrecondition;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Validates that a committed result is the exact logical append accepted from the caller. */
public final class AppendResultValidator {
    private AppendResultValidator() {
    }

    public static AppendResult requireExactRequest(
            StreamId streamId,
            AppendBatch request,
            AppendPrecondition precondition,
            AppendResult result) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(precondition, "precondition");
        Objects.requireNonNull(result, "result");
        long logicalBytes = logicalBytes(request);
        boolean expectedStartMatches = precondition.expectedStartOffset().isEmpty()
                || precondition.expectedStartOffset().getAsLong() == result.range().startOffset();
        if (!result.streamId().equals(streamId)
                || result.payloadFormat() != request.payloadFormat()
                || result.recordCount() != request.recordCount()
                || result.entryCount() != request.entryCount()
                || result.logicalBytes() != logicalBytes
                || !result.schemaRefs().equals(request.schemaRefs())
                || result.range().recordCount() != request.recordCount()
                || !expectedStartMatches) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "committed append result does not exactly match the accepted request",
                    AppendOutcome.KNOWN_COMMITTED);
        }
        return result;
    }

    private static long logicalBytes(AppendBatch request) {
        long logicalBytes = 0;
        try {
            for (var entry : request.entries()) {
                logicalBytes = Math.addExact(logicalBytes, entry.payload().length);
            }
            return logicalBytes;
        } catch (ArithmeticException overflow) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "accepted append logical byte count overflows",
                    overflow,
                    AppendOutcome.KNOWN_COMMITTED);
        }
    }
}
