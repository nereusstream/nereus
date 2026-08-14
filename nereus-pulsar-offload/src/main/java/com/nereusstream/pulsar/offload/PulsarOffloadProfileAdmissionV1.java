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

package com.nereusstream.pulsar.offload;

/** Fail-closed provider admission for one predeclared limits candidate. */
public final class PulsarOffloadProfileAdmissionV1 {
    private PulsarOffloadProfileAdmissionV1() {}

    public static void requireAdmitted(
            PulsarOffloadLimitCandidateV1 candidate, PulsarOffloadObjectStoreV1.Capabilities capabilities) {
        if (!capabilities.immutableConditionalCreate()
                || !capabilities.streamingUpload()
                || !capabilities.boundedRangeRead()
                || !capabilities.boundedFullRead()
                || !capabilities.deterministicMultipartCleanup()) {
            throw new IllegalArgumentException("provider lacks a mandatory Pulsar offload capability");
        }
        if (capabilities.maximumObjectBytes() < candidate.maxDataObjectBytes()
                || capabilities.maximumPartCount() < candidate.maxMultipartParts()) {
            throw new IllegalArgumentException("provider Object or part-count capacity is below the candidate");
        }
        long multipartCapacity;
        try {
            multipartCapacity =
                    Math.multiplyExact(capabilities.maximumPartBytes(), (long) candidate.maxMultipartParts());
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("provider multipart capacity overflows", failure);
        }
        if (multipartCapacity < candidate.maxDataObjectBytes()
                || capabilities.minimumPartBytes() > capabilities.maximumPartBytes()) {
            throw new IllegalArgumentException("provider multipart interval cannot cover the candidate");
        }
    }
}
