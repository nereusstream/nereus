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

package io.nereus.metadata.oxia;

import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectType;
import io.nereus.metadata.oxia.records.EntryIndexReferenceRecord;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.StreamSliceManifestRecord;
import java.util.HashSet;
import java.util.Set;

/** Shared Phase 1 manifest validation used by fake and future real Oxia adapters. */
public final class Phase1ObjectManifestValidator {
    private static final Set<String> COMMIT_ELIGIBLE_MANIFEST_STATES =
            Set.of("UPLOADED", "PARTIALLY_VISIBLE", "VISIBLE");
    private static final Set<String> SLICE_STATES = Set.of("UPLOADED", "VISIBLE");

    private Phase1ObjectManifestValidator() {
    }

    public static void validateStoredManifest(ObjectManifestRecord manifest) {
        validateStoredShape(manifest);
    }

    public static void validateNewUpload(ObjectManifestRecord manifest) {
        validateStoredManifest(manifest);
        if (!"UPLOADED".equals(manifest.state())
                || manifest.slices().stream().anyMatch(slice -> !"UPLOADED".equals(slice.state()))) {
            throw invariant("new object manifest and every slice must be UPLOADED");
        }
    }

    public static StreamSliceManifestRecord validateCommitCandidate(
            ObjectManifestRecord manifest,
            CommitSliceRequest request,
            boolean replay) {
        validateStoredManifest(manifest);
        if (!COMMIT_ELIGIBLE_MANIFEST_STATES.contains(manifest.state())) {
            throw invariant("object manifest state is not commit eligible");
        }
        if (!manifest.objectId().equals(request.objectId().value())
                || !manifest.objectKey().equals(request.objectKey().value())
                || !manifest.writerId().equals(request.writerId())
                || !manifest.writerRunIdHash().equals(request.writerRunIdHash())
                || manifest.writerEpoch() != request.epoch()
                || !manifest.objectChecksumType().equals(request.objectChecksum().type().name())
                || !manifest.objectChecksumValue().equals(request.objectChecksum().value())) {
            throw invariant("object manifest does not match commit request");
        }

        StreamSliceManifestRecord slice = manifest.slices().stream()
                .filter(candidate -> candidate.sliceId().equals(request.sliceId()))
                .findFirst()
                .orElseThrow(() -> invariant("object manifest slice is missing"));
        if (!replay && !"UPLOADED".equals(slice.state())) {
            throw invariant("new commit requires uploaded manifest slice");
        }
        if (!slice.streamId().equals(request.streamId().value())
                || slice.writerEpoch() != request.epoch()
                || slice.objectOffset() != request.objectOffset()
                || slice.objectLength() != request.objectLength()
                || slice.recordCount() != request.recordCount()
                || slice.entryCount() != request.entryCount()
                || slice.logicalBytes() != request.logicalBytes()
                || !slice.payloadFormat().equals(request.payloadFormat().name())
                || !slice.schemaRefs().equals(request.schemaRefs())
                || !slice.entryIndexRef().equals(EntryIndexReferenceRecord.fromApi(request.entryIndexRef()))
                || !slice.sliceChecksumType().equals(request.sliceChecksum().type().name())
                || !slice.sliceChecksumValue().equals(request.sliceChecksum().value())) {
            throw invariant("object manifest slice does not match commit request");
        }
        return slice;
    }

    public static boolean sameImmutableIdentity(ObjectManifestRecord left, ObjectManifestRecord right) {
        validateStoredManifest(left);
        validateStoredManifest(right);
        if (!left.objectId().equals(right.objectId())
                || !left.objectKey().equals(right.objectKey())
                || !left.objectType().equals(right.objectType())
                || left.formatMajorVersion() != right.formatMajorVersion()
                || left.formatMinorVersion() != right.formatMinorVersion()
                || !left.writerVersion().equals(right.writerVersion())
                || !left.writerId().equals(right.writerId())
                || !left.writerRunIdHash().equals(right.writerRunIdHash())
                || left.writerEpoch() != right.writerEpoch()
                || left.objectLength() != right.objectLength()
                || !left.objectChecksumType().equals(right.objectChecksumType())
                || !left.objectChecksumValue().equals(right.objectChecksumValue())
                || !left.storageChecksumType().equals(right.storageChecksumType())
                || !left.storageChecksumValue().equals(right.storageChecksumValue())
                || left.slices().size() != right.slices().size()) {
            return false;
        }
        for (int index = 0; index < left.slices().size(); index++) {
            if (!sameImmutableSliceIdentity(left.slices().get(index), right.slices().get(index))) {
                return false;
            }
        }
        return true;
    }

    private static void validateStoredShape(ObjectManifestRecord manifest) {
        if (!ObjectType.MULTI_STREAM_WAL_OBJECT.name().equals(manifest.objectType())
                || manifest.formatMajorVersion() != 1
                || manifest.formatMinorVersion() != 0
                || manifest.objectLength() <= 0
                || manifest.slices().isEmpty()) {
            throw invariant("unsupported or invalid Phase 1 object manifest shape");
        }
        Set<String> sliceIds = new HashSet<>();
        Set<Integer> sliceOrdinals = new HashSet<>();
        int visibleSlices = 0;
        long previousRangeEnd = -1;
        for (int index = 0; index < manifest.slices().size(); index++) {
            StreamSliceManifestRecord slice = manifest.slices().get(index);
            if (!sliceIds.add(slice.sliceId()) || !sliceOrdinals.add(slice.sliceOrdinal())) {
                throw invariant("object manifest slice ids and ordinals must be unique");
            }
            if (slice.sliceOrdinal() != index) {
                throw invariant("object manifest slice ordinal must match encoded list order");
            }
            if (!SLICE_STATES.contains(slice.state())) {
                throw invariant("object manifest contains unsupported slice state");
            }
            if (slice.objectLength() <= 0) {
                throw invariant("object manifest slice length must be positive");
            }
            if ("VISIBLE".equals(slice.state())) {
                visibleSlices++;
            }
            long rangeEnd;
            try {
                rangeEnd = Math.addExact(slice.objectOffset(), slice.objectLength());
            } catch (ArithmeticException e) {
                throw invariant("object manifest slice range overflows", e);
            }
            if (rangeEnd > manifest.objectLength()) {
                throw invariant("object manifest slice range exceeds object length");
            }
            if (slice.objectOffset() < previousRangeEnd) {
                throw invariant("object manifest slice ranges must be ordered and non-overlapping");
            }
            previousRangeEnd = rangeEnd;
        }
        boolean consistentState = switch (manifest.state()) {
            case "UPLOADED" -> visibleSlices == 0;
            case "PARTIALLY_VISIBLE" -> visibleSlices > 0 && visibleSlices < manifest.slices().size();
            case "VISIBLE" -> visibleSlices == manifest.slices().size();
            default -> false;
        };
        if (!consistentState) {
            throw invariant("object manifest state does not match per-slice visibility");
        }
    }

    private static boolean sameImmutableSliceIdentity(
            StreamSliceManifestRecord left,
            StreamSliceManifestRecord right) {
        return left.sliceOrdinal() == right.sliceOrdinal()
                && left.streamId().equals(right.streamId())
                && left.sliceId().equals(right.sliceId())
                && left.writerEpoch() == right.writerEpoch()
                && left.objectOffset() == right.objectOffset()
                && left.objectLength() == right.objectLength()
                && left.recordCount() == right.recordCount()
                && left.entryCount() == right.entryCount()
                && left.logicalBytes() == right.logicalBytes()
                && left.schemaRefs().equals(right.schemaRefs())
                && left.entryIndexRef().equals(right.entryIndexRef())
                && left.sliceChecksumType().equals(right.sliceChecksumType())
                && left.sliceChecksumValue().equals(right.sliceChecksumValue())
                && left.payloadFormat().equals(right.payloadFormat());
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }
}
