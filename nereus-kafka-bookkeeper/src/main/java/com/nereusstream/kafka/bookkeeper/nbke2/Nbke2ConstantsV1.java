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

package com.nereusstream.kafka.bookkeeper.nbke2;

/** Persisted NBKE2 major-version-one constants. Changing one requires a new accepted wire version. */
public final class Nbke2ConstantsV1 {
    public static final byte[] MAGIC = {0x4e, 0x42, 0x4b, 0x45, 0x32};
    public static final int MAJOR_VERSION = 1;
    public static final int MINOR_VERSION = 0;
    public static final int FIXED_HEADER_BYTES = 32;
    public static final int CRC32C_BYTES = 4;
    public static final int SHA256_BYTES = 32;
    public static final int DATA_TERMINAL_DESCRIPTOR_FLAG = 0x01;
    public static final int KNOWN_DATA_FLAGS = DATA_TERMINAL_DESCRIPTOR_FLAG;

    public static final int FORMAT_MAX_FRAME_BYTES = 8 * 1024 * 1024;
    public static final int FORMAT_MAX_DATA_PAYLOAD_BYTES = 8 * 1024 * 1024 - 1024;
    public static final int FORMAT_MAX_TOPIC_NAME_BYTES = 249;
    public static final int FORMAT_MAX_LOCATOR_COUNT = 65_536;
    public static final int FORMAT_MAX_INDEX_DIRECTORY_COUNT = 65_536;
    public static final int FORMAT_MAX_CHECKPOINT_SECTION_BYTES = 2 * 1024 * 1024;
    public static final int LOCATOR_BYTES = 32;
    public static final int INDEX_DIRECTORY_ENTRY_BYTES = 24;
    public static final int APPEND_GROUP_DESCRIPTOR_BYTES = 64;

    private Nbke2ConstantsV1() {}
}
