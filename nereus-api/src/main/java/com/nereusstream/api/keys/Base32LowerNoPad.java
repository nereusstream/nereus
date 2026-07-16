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

package com.nereusstream.api.keys;

final class Base32LowerNoPad {
    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz234567".toCharArray();

    private Base32LowerNoPad() {
    }

    static String encode(byte[] bytes) {
        StringBuilder result = new StringBuilder((bytes.length * 8 + 4) / 5);
        int buffer = 0;
        int bitsLeft = 0;
        for (byte b : bytes) {
            buffer = (buffer << 8) | (b & 0xff);
            bitsLeft += 8;
            while (bitsLeft >= 5) {
                int index = (buffer >> (bitsLeft - 5)) & 0x1f;
                bitsLeft -= 5;
                result.append(ALPHABET[index]);
            }
        }
        if (bitsLeft > 0) {
            int index = (buffer << (5 - bitsLeft)) & 0x1f;
            result.append(ALPHABET[index]);
        }
        return result.toString();
    }

    static byte[] decode(String value) {
        byte[] result = new byte[value.length() * 5 / 8];
        int outputIndex = 0;
        int buffer = 0;
        int bits = 0;
        for (int index = 0; index < value.length(); index++) {
            int decoded = decode(value.charAt(index));
            if (decoded < 0) {
                throw new IllegalArgumentException("base32 value contains a non-canonical character");
            }
            buffer = (buffer << 5) | decoded;
            bits += 5;
            if (bits >= 8) {
                bits -= 8;
                result[outputIndex++] = (byte) ((buffer >>> bits) & 0xff);
                buffer &= (1 << bits) - 1;
            }
        }
        if (bits > 0 && buffer != 0) {
            throw new IllegalArgumentException("base32 value has non-zero trailing bits");
        }
        if (outputIndex != result.length) {
            throw new IllegalArgumentException("base32 value has an invalid length");
        }
        return result;
    }

    private static int decode(char value) {
        if (value >= 'a' && value <= 'z') {
            return value - 'a';
        }
        if (value >= '2' && value <= '7') {
            return value - '2' + 26;
        }
        return -1;
    }
}
