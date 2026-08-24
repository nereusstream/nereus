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

package com.nereusstream.storage.object.control;

import com.nereusstream.storage.object.nwg1.Nwg1ConstantsV1;

/** Root-persisted NWG1 admission ceilings. Each value can only narrow the immutable v1 format-hard ceiling. */
public record Nwg1RootAdmissionCaps(
        long maxCanonicalBodyBytes,
        int maxDirectoryPrefixBytes,
        int maxDirectoryPlaintextBytes,
        int maxBindingContexts,
        int maxAppendUnits,
        int maxFrames,
        int maxDecodedFrameBytes,
        int maxStoredFrameBytes,
        long maxDecodedAppendUnitBytes,
        long maxTotalDecodedPayloadBytes) {
    public Nwg1RootAdmissionCaps {
        if (maxCanonicalBodyBytes <= 0 || maxCanonicalBodyBytes > Nwg1ConstantsV1.MAX_CANONICAL_BODY_BYTES) {
            throw new IllegalArgumentException("Root body cap lies outside NWG1 v1");
        }
        if (maxDirectoryPrefixBytes <= 0
                || maxDirectoryPrefixBytes > Nwg1ConstantsV1.MAX_DIRECTORY_PREFIX_BYTES
                || maxDirectoryPrefixBytes > maxCanonicalBodyBytes) {
            throw new IllegalArgumentException("Root directory-prefix cap lies outside NWG1 v1/body");
        }
        if (maxDirectoryPlaintextBytes <= 0
                || maxDirectoryPlaintextBytes > Nwg1ConstantsV1.MAX_DIRECTORY_PLAINTEXT_BYTES) {
            throw new IllegalArgumentException("Root directory-plaintext cap lies outside NWG1 v1");
        }
        long minimumStoredDirectory = Math.addExact(
                Math.addExact((long) maxDirectoryPlaintextBytes, Nwg1ConstantsV1.HEADER_BYTES),
                Nwg1ConstantsV1.GCM_TAG_BYTES);
        if (minimumStoredDirectory > maxDirectoryPrefixBytes) {
            throw new IllegalArgumentException("Root directory plaintext/header/tag cannot fit its prefix cap");
        }
        if (maxBindingContexts <= 0 || maxBindingContexts > Nwg1ConstantsV1.MAX_BINDING_CONTEXTS) {
            throw new IllegalArgumentException("Root binding-context cap lies outside NWG1 v1");
        }
        if (maxAppendUnits <= 0 || maxAppendUnits > Nwg1ConstantsV1.MAX_APPEND_UNITS) {
            throw new IllegalArgumentException("Root append-unit cap lies outside NWG1 v1");
        }
        if (maxFrames <= 0 || maxFrames > Nwg1ConstantsV1.MAX_FRAMES) {
            throw new IllegalArgumentException("Root frame cap lies outside NWG1 v1");
        }
        if (maxDecodedFrameBytes <= 0 || maxDecodedFrameBytes > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES) {
            throw new IllegalArgumentException("Root decoded-frame cap lies outside NWG1 v1");
        }
        if (maxStoredFrameBytes != Math.addExact(maxDecodedFrameBytes, Nwg1ConstantsV1.GCM_TAG_BYTES)
                || maxStoredFrameBytes > Nwg1ConstantsV1.MAX_STORED_FRAME_BYTES
                || maxStoredFrameBytes > maxCanonicalBodyBytes) {
            throw new IllegalArgumentException("Root stored-frame cap is inconsistent with decoded frame plus tag");
        }
        if (maxDecodedAppendUnitBytes <= 0
                || maxDecodedAppendUnitBytes > Nwg1ConstantsV1.MAX_TOTAL_DECODED_PAYLOAD_BYTES
                || maxDecodedAppendUnitBytes < maxDecodedFrameBytes) {
            throw new IllegalArgumentException("Root decoded-append-unit cap lies outside NWG1 v1/frame");
        }
        if (maxTotalDecodedPayloadBytes <= 0
                || maxTotalDecodedPayloadBytes > Nwg1ConstantsV1.MAX_TOTAL_DECODED_PAYLOAD_BYTES
                || maxTotalDecodedPayloadBytes < maxDecodedAppendUnitBytes) {
            throw new IllegalArgumentException("Root decoded-payload cap lies outside NWG1 v1/append unit");
        }
    }

    public static Nwg1RootAdmissionCaps formatHardCaps() {
        return new Nwg1RootAdmissionCaps(
                Nwg1ConstantsV1.MAX_CANONICAL_BODY_BYTES,
                Nwg1ConstantsV1.MAX_DIRECTORY_PREFIX_BYTES,
                Nwg1ConstantsV1.MAX_DIRECTORY_PLAINTEXT_BYTES,
                Nwg1ConstantsV1.MAX_BINDING_CONTEXTS,
                Nwg1ConstantsV1.MAX_APPEND_UNITS,
                Nwg1ConstantsV1.MAX_FRAMES,
                Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES,
                Nwg1ConstantsV1.MAX_STORED_FRAME_BYTES,
                Nwg1ConstantsV1.MAX_TOTAL_DECODED_PAYLOAD_BYTES,
                Nwg1ConstantsV1.MAX_TOTAL_DECODED_PAYLOAD_BYTES);
    }
}
