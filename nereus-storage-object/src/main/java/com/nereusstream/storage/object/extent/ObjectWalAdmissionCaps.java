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

/** Evidence-selected local/Provider caps, each no greater than the NWG1 format-hard cap. */
public record ObjectWalAdmissionCaps(
        long maximumBodyBytes,
        int maximumDirectoryPrefixBytes,
        int maximumDirectoryPlaintextBytes,
        long maximumTotalDecodedBytes) {
    public ObjectWalAdmissionCaps {
        if (maximumBodyBytes <= 0 || maximumBodyBytes > ObjectWalFormatCaps.MAX_BODY_BYTES) {
            throw new IllegalArgumentException("body admission cap lies outside the format bound");
        }
        if (maximumDirectoryPrefixBytes <= 0
                || maximumDirectoryPrefixBytes > ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES
                || maximumDirectoryPrefixBytes > maximumBodyBytes) {
            throw new IllegalArgumentException("directory prefix admission cap lies outside the format/body bound");
        }
        if (maximumDirectoryPlaintextBytes <= 0
                || maximumDirectoryPlaintextBytes > ObjectWalFormatCaps.MAX_DIRECTORY_PLAINTEXT_BYTES) {
            throw new IllegalArgumentException("directory plaintext admission cap lies outside the format bound");
        }
        if (maximumTotalDecodedBytes <= 0 || maximumTotalDecodedBytes > ObjectWalFormatCaps.MAX_TOTAL_DECODED_BYTES) {
            throw new IllegalArgumentException("decoded admission cap lies outside the format bound");
        }
    }
}
