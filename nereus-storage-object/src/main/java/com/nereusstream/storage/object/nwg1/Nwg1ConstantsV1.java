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
package com.nereusstream.storage.object.nwg1;

import java.nio.charset.StandardCharsets;

/** Frozen NWG1 v1 widths, domains, codes, and absolute parser ceilings. */
public final class Nwg1ConstantsV1 {
    public static final byte[] HEADER_MAGIC = "NWG1".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] DIRECTORY_MAGIC = "NWD1".getBytes(StandardCharsets.US_ASCII);
    public static final int WIRE_VERSION = 1;
    public static final int HEADER_BYTES = 256;
    public static final int DIRECTORY_PREAMBLE_BYTES = 32;
    public static final int BINDING_ROW_BYTES = 116;
    public static final int KAFKA_APPEND_UNIT_ROW_BYTES = 104;
    public static final int PULSAR_APPEND_UNIT_ROW_BYTES = 96;
    public static final int FRAME_ROW_BYTES = 48;
    public static final int OBJECT_KEY_INFO_BYTES = 37;
    public static final int NONCE_BYTES = 12;
    public static final int GCM_TAG_BYTES = 16;
    public static final int DIRECTORY_AAD_BYTES = 272;
    public static final int FRAME_AAD_BYTES = 328;

    public static final long MAX_CANONICAL_BODY_BYTES = 4_294_967_296L;
    public static final int MAX_DIRECTORY_PREFIX_BYTES = 4_194_304;
    public static final int MAX_DIRECTORY_PLAINTEXT_BYTES = 4_194_032;
    public static final int MAX_BINDING_CONTEXTS = 256;
    public static final int MAX_APPEND_UNITS = 65_536;
    public static final int MAX_FRAMES = 65_536;
    public static final int MAX_NTI1_BYTES = 8_214;
    public static final int MAX_DECODED_FRAME_BYTES = 67_108_864;
    public static final int MAX_STORED_FRAME_BYTES = 67_108_880;
    public static final long MAX_TOTAL_DECODED_PAYLOAD_BYTES = 4_294_967_296L;

    public static final int PROTOCOL_KAFKA = 1;
    public static final int PROTOCOL_PULSAR = 2;
    public static final int CODEC_NONE_KIND = 0;
    public static final int CODEC_NONE_VERSION = 0;
    public static final int CODEC_ZSTD_KIND = 1;
    public static final int CODEC_ZSTD_VERSION = 1;
    public static final int CLOSED_KIND = 1;
    public static final int CLOSED_VERSION = 1;

    public static final byte[] OBJECT_KEY_DOMAIN = exactDomain("NWG1/OBJ/KEY/V1\0");
    public static final byte[] DIRECTORY_AAD_DOMAIN = exactDomain("NWG1/DIR/AAD/V1\0");
    public static final byte[] FRAME_AAD_DOMAIN = exactDomain("NWG1/FRM/AAD/V1\0");
    public static final byte[] CELL_COMMITMENT_DOMAIN = exactDomain("NWG1/CELL/ID/V1\0");
    public static final byte[] OWNER_COMMITMENT_DOMAIN = ascii("NWG1/OWNER-FENCE/V1\0");
    public static final byte[] ENVELOPE_COMMITMENT_DOMAIN = exactDomain("NWG1/KEY/ENV/V1\0");
    public static final byte[] MUTATION_ROOT_DOMAIN = ascii("NWG1/MUTATION/ROOT/V1\0");

    private Nwg1ConstantsV1() {}

    private static byte[] exactDomain(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        if (bytes.length != 16) {
            throw new ExceptionInInitializerError("NWG1 domain must be 16 bytes: " + value);
        }
        return bytes;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
