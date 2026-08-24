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

package com.nereusstream.storage.object.extent;

import com.nereusstream.storage.object.nwg1.Nwg1ConstantsV1;

/** NWG1 v1 format-hard caps used by local D1 checked admission. */
public final class ObjectWalFormatCaps {
    public static final long MAX_BODY_BYTES = Nwg1ConstantsV1.MAX_CANONICAL_BODY_BYTES;
    public static final int MAX_DIRECTORY_PREFIX_BYTES = Nwg1ConstantsV1.MAX_DIRECTORY_PREFIX_BYTES;
    public static final int MAX_DIRECTORY_PLAINTEXT_BYTES = Nwg1ConstantsV1.MAX_DIRECTORY_PLAINTEXT_BYTES;
    public static final int MAX_CONTEXTS = Nwg1ConstantsV1.MAX_BINDING_CONTEXTS;
    public static final int MAX_APPEND_UNITS = Nwg1ConstantsV1.MAX_APPEND_UNITS;
    public static final int MAX_FRAMES = Nwg1ConstantsV1.MAX_FRAMES;
    public static final int MAX_FRAME_DECODED_BYTES = Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES;
    public static final int MAX_FRAME_STORED_BYTES = Nwg1ConstantsV1.MAX_STORED_FRAME_BYTES;
    public static final long MAX_TOTAL_DECODED_BYTES = Nwg1ConstantsV1.MAX_TOTAL_DECODED_PAYLOAD_BYTES;

    private ObjectWalFormatCaps() {}
}
