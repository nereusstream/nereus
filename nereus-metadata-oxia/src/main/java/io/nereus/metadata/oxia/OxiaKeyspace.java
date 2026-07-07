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

import io.nereus.api.ObjectId;
import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import io.nereus.api.keys.DeterministicIds;
import io.nereus.api.keys.KeyComponentCodec;
import java.util.Objects;

/** Durable Oxia key builder for Phase 1 metadata. */
public final class OxiaKeyspace {
    private static final String ROOT = "/nereus/clusters/";

    private final String cluster;
    private final String clusterComponent;
    private final String prefix;

    public OxiaKeyspace(String cluster) {
        this.cluster = requireNonBlank(cluster, "cluster");
        this.clusterComponent = KeyComponentCodec.encodeComponent(cluster);
        this.prefix = ROOT + clusterComponent;
    }

    public String cluster() {
        return cluster;
    }

    public String prefix() {
        return prefix;
    }

    public PartitionKey streamPartitionKey(StreamId streamId) {
        return new PartitionKey(Objects.requireNonNull(streamId, "streamId").value());
    }

    public PartitionKey objectPartitionKey(ObjectId objectId) {
        return new PartitionKey(Objects.requireNonNull(objectId, "objectId").value());
    }

    public String streamHeadKey(StreamId streamId) {
        return streamPrefix(streamId) + "/head";
    }

    public String streamCommitKey(StreamId streamId, String commitId) {
        return streamPrefix(streamId) + "/commit-log/" + KeyComponentCodec.encodeComponent(commitId);
    }

    public String offsetIndexKey(StreamId streamId, long offsetEnd, long generation) {
        return offsetIndexPrefix(streamId)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(offsetEnd)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(generation);
    }

    public String offsetIndexScanFromExclusive(StreamId streamId, long targetOffset) {
        return offsetIndexPrefix(streamId)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(targetOffset)
                + "/~";
    }

    public String offsetIndexScanToExclusive(StreamId streamId) {
        return offsetIndexPrefix(streamId) + "/~";
    }

    public String committedSliceKey(StreamId streamId, ObjectId objectId, String sliceId) {
        Objects.requireNonNull(objectId, "objectId");
        String sliceComponent = DeterministicIds.stableHashComponent(objectId.value() + "\0" + requireNonBlank(sliceId, "sliceId"));
        return streamPrefix(streamId)
                + "/committed-slices/"
                + KeyComponentCodec.encodeComponent(objectId.value())
                + "/"
                + sliceComponent;
    }

    public String streamNameKey(StreamName streamName) {
        return prefix + "/streams/by-name/" + DeterministicIds.streamNameHash(streamName);
    }

    public String objectManifestKey(ObjectId objectId) {
        return objectPrefix(objectId) + "/manifest";
    }

    public String objectReferencesKey(ObjectId objectId) {
        return objectPrefix(objectId) + "/references";
    }

    public String objectGcKey(ObjectId objectId) {
        return objectPrefix(objectId) + "/gc";
    }

    private String streamPrefix(StreamId streamId) {
        Objects.requireNonNull(streamId, "streamId");
        return prefix + "/streams/" + KeyComponentCodec.encodeComponent(streamId.value());
    }

    private String offsetIndexPrefix(StreamId streamId) {
        return streamPrefix(streamId) + "/offset-index";
    }

    private String objectPrefix(ObjectId objectId) {
        Objects.requireNonNull(objectId, "objectId");
        return prefix + "/objects/" + KeyComponentCodec.encodeComponent(objectId.value());
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
