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

package com.nereusstream.domain.registry.allocator;

/** Closed attachment inventory required by one allocator selection receipt. */
public enum AllocatorEvidenceAttachmentKindV1 {
    TEST(1, "test.naea"),
    NATIVE(2, "native.naea"),
    FAULT(3, "fault.naea"),
    SCALE_10K(4, "scale-10000.naea"),
    SCALE_100K(5, "scale-100000.naea");

    private final int code;
    private final String fileName;

    AllocatorEvidenceAttachmentKindV1(int code, String fileName) {
        this.code = code;
        this.fileName = fileName;
    }

    int code() {
        return code;
    }

    /** Exact basename in the closed five-file NAEA1 inventory. */
    public String fileName() {
        return fileName;
    }

    static AllocatorEvidenceAttachmentKindV1 fromCode(int code) {
        for (AllocatorEvidenceAttachmentKindV1 value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw AllocatorSelectionReceiptV1.invalid("unknown allocator evidence attachment kind");
    }
}
