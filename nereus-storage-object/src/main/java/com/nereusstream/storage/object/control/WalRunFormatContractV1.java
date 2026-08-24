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

/** Closed Root authority for NWG1, binding validation, leaf/lane sequencing, and cryptographic families. */
public record WalRunFormatContractV1(
        int nwg1ManifestVersion,
        int headerLayoutVersion,
        int directoryLayoutVersion,
        int bindingContextRowVersion,
        int kafkaAppendUnitRowVersion,
        int pulsarAppendUnitRowVersion,
        int commonFrameRowVersion,
        int bindingEpochValidationKind,
        int bindingEpochValidationVersion,
        int leafKeyGrammarVersion,
        int laneCatalogVersion,
        int planThenSequenceContractVersion,
        int packingPolicyCatalogVersion,
        int frameCodecRegistryKind,
        int frameCodecRegistryVersion,
        int allowedFrameCodecsVersion,
        int objectDigestKind,
        int objectDigestVersion,
        int payloadChecksumKind,
        int payloadChecksumVersion,
        int aeadKind,
        int aeadVersion,
        int kdfKind,
        int kdfVersion,
        int nonceLayoutVersion,
        int rootEnvelopeKind,
        int rootEnvelopeVersion) {
    public static final int FROZEN_V1_CODE = 1;

    public WalRunFormatContractV1 {
        if (nwg1ManifestVersion != FROZEN_V1_CODE
                || headerLayoutVersion != FROZEN_V1_CODE
                || directoryLayoutVersion != FROZEN_V1_CODE
                || bindingContextRowVersion != FROZEN_V1_CODE
                || kafkaAppendUnitRowVersion != FROZEN_V1_CODE
                || pulsarAppendUnitRowVersion != FROZEN_V1_CODE
                || commonFrameRowVersion != FROZEN_V1_CODE
                || bindingEpochValidationKind != FROZEN_V1_CODE
                || bindingEpochValidationVersion != FROZEN_V1_CODE
                || leafKeyGrammarVersion != FROZEN_V1_CODE
                || laneCatalogVersion != FROZEN_V1_CODE
                || planThenSequenceContractVersion != FROZEN_V1_CODE
                || packingPolicyCatalogVersion != FROZEN_V1_CODE
                || frameCodecRegistryKind != FROZEN_V1_CODE
                || frameCodecRegistryVersion != FROZEN_V1_CODE
                || allowedFrameCodecsVersion != FROZEN_V1_CODE
                || objectDigestKind != FROZEN_V1_CODE
                || objectDigestVersion != FROZEN_V1_CODE
                || payloadChecksumKind != FROZEN_V1_CODE
                || payloadChecksumVersion != FROZEN_V1_CODE
                || aeadKind != FROZEN_V1_CODE
                || aeadVersion != FROZEN_V1_CODE
                || kdfKind != FROZEN_V1_CODE
                || kdfVersion != FROZEN_V1_CODE
                || nonceLayoutVersion != FROZEN_V1_CODE
                || rootEnvelopeKind != FROZEN_V1_CODE
                || rootEnvelopeVersion != FROZEN_V1_CODE) {
            throw new IllegalArgumentException("WalRun format contract contains an unknown v1 family/version");
        }
    }

    public static WalRunFormatContractV1 frozen() {
        return new WalRunFormatContractV1(
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    }
}
