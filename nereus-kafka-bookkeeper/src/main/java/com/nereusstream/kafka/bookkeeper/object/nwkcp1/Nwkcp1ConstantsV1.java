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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

/** Frozen NWKCP1 v1 wire, key, and allocation caps. */
public final class Nwkcp1ConstantsV1 {
    /** ASCII {@code NWKCP1}, a zero byte, and the binary version one. */
    public static final long MAGIC = 0x4e_57_4b_43_50_31_00_01L;

    /** ASCII {@code NWKH1}, two zero bytes, and the binary version one. */
    public static final long HEAD_MAGIC = 0x4e_57_4b_48_31_00_00_01L;

    public static final int HEADER_BYTES = 64;
    public static final int FORMAT_MAX_ROWS = 256;
    public static final int FORMAT_MAX_TOPIC_NAME_BYTES = 249;
    public static final int FORMAT_MAX_SECTION_BYTES = 2 * 1024 * 1024;
    public static final int FORMAT_MAX_ROW_BYTES = 8 * 1024 * 1024;
    public static final int FORMAT_MAX_OBJECT_BYTES = 64 * 1024 * 1024;
    public static final int FORMAT_MAX_KEY_BYTES = 1_024;
    public static final int FORMAT_MAX_HEAD_BYTES = 128 * 1024;
    public static final int FORMAT_MAX_HEAD_VECTOR_ROWS = FORMAT_MAX_ROWS;
    public static final int HEAD_VECTOR_ROW_BYTES = 128;
    public static final int RECONCILE_MAX_LIST_PAGES = 16;
    public static final long RECONCILE_MAX_LIST_KEYS = 4_096;
    public static final long RECONCILE_MAX_LIST_KEY_BYTES = 4L * 1024 * 1024;

    public static final String FAMILY_PREFIX = "protocol/kafka/nwkcp1-v1";
    public static final String OBJECTS_TOKEN = "objects";
    public static final String HEAD_TOKEN = "head";
    public static final String DIGEST_TOKEN = "sha256-v1-";
    public static final String OBJECT_SUFFIX = ".nwkcp1";

    private Nwkcp1ConstantsV1() {}
}
