/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.ImplicitLinkedHashMultiCollection;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class EndQuorumEpochRequestData implements ApiMessage {
    String clusterId;
    List<TopicData> topics;
    LeaderEndpointCollection leaderEndpoints;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("cluster_id", Type.NULLABLE_STRING, "The cluster id."),
            new Field("topics", new ArrayOf(TopicData.SCHEMA_0), "The topics.")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("cluster_id", Type.COMPACT_NULLABLE_STRING, "The cluster id."),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_1), "The topics."),
            new Field("leader_endpoints", new CompactArrayOf(LeaderEndpoint.SCHEMA_1), "Endpoints for the leader."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 1;
    
    public EndQuorumEpochRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public EndQuorumEpochRequestData() {
        this.clusterId = null;
        this.topics = new ArrayList<TopicData>(0);
        this.leaderEndpoints = new LeaderEndpointCollection(0);
    }
    
    @Override
    public short apiKey() {
        return 54;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 1;
    }
    
    @Override
    public final void read(Readable _readable, short _version) {
        {
            int length;
            if (_version >= 1) {
                length = _readable.readUnsignedVarint() - 1;
            } else {
                length = _readable.readShort();
            }
            if (length < 0) {
                this.clusterId = null;
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field clusterId had invalid length " + length);
            } else {
                this.clusterId = _readable.readString(length);
            }
        }
        {
            if (_version >= 1) {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field topics was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<TopicData> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new TopicData(_readable, _version));
                    }
                    this.topics = newCollection;
                }
            } else {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field topics was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<TopicData> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new TopicData(_readable, _version));
                    }
                    this.topics = newCollection;
                }
            }
        }
        if (_version >= 1) {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field leaderEndpoints was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                LeaderEndpointCollection newCollection = new LeaderEndpointCollection(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new LeaderEndpoint(_readable, _version));
                }
                this.leaderEndpoints = newCollection;
            }
        } else {
            this.leaderEndpoints = new LeaderEndpointCollection(0);
        }
        this._unknownTaggedFields = null;
        if (_version >= 1) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (clusterId == null) {
            if (_version >= 1) {
                _writable.writeUnsignedVarint(0);
            } else {
                _writable.writeShort((short) -1);
            }
        } else {
            byte[] _stringBytes = _cache.getSerializedValue(clusterId);
            if (_version >= 1) {
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
            } else {
                _writable.writeShort((short) _stringBytes.length);
            }
            _writable.writeByteArray(_stringBytes);
        }
        if (_version >= 1) {
            _writable.writeUnsignedVarint(topics.size() + 1);
            for (TopicData topicsElement : topics) {
                topicsElement.write(_writable, _cache, _version);
            }
        } else {
            _writable.writeInt(topics.size());
            for (TopicData topicsElement : topics) {
                topicsElement.write(_writable, _cache, _version);
            }
        }
        if (_version >= 1) {
            _writable.writeUnsignedVarint(leaderEndpoints.size() + 1);
            for (LeaderEndpoint leaderEndpointsElement : leaderEndpoints) {
                leaderEndpointsElement.write(_writable, _cache, _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 1) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (clusterId == null) {
            if (_version >= 1) {
                _size.addBytes(1);
            } else {
                _size.addBytes(2);
            }
        } else {
            byte[] _stringBytes = clusterId.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'clusterId' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(clusterId, _stringBytes);
            if (_version >= 1) {
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            } else {
                _size.addBytes(_stringBytes.length + 2);
            }
        }
        {
            if (_version >= 1) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(topics.size() + 1));
            } else {
                _size.addBytes(4);
            }
            for (TopicData topicsElement : topics) {
                topicsElement.addSize(_size, _cache, _version);
            }
        }
        if (_version >= 1) {
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(leaderEndpoints.size() + 1));
                for (LeaderEndpoint leaderEndpointsElement : leaderEndpoints) {
                    leaderEndpointsElement.addSize(_size, _cache, _version);
                }
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        if (_version >= 1) {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EndQuorumEpochRequestData)) return false;
        EndQuorumEpochRequestData other = (EndQuorumEpochRequestData) obj;
        if (this.clusterId == null) {
            if (other.clusterId != null) return false;
        } else {
            if (!this.clusterId.equals(other.clusterId)) return false;
        }
        if (this.topics == null) {
            if (other.topics != null) return false;
        } else {
            if (!this.topics.equals(other.topics)) return false;
        }
        if (this.leaderEndpoints == null) {
            if (other.leaderEndpoints != null) return false;
        } else {
            if (!this.leaderEndpoints.equals(other.leaderEndpoints)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (clusterId == null ? 0 : clusterId.hashCode());
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        hashCode = 31 * hashCode + (leaderEndpoints == null ? 0 : leaderEndpoints.hashCode());
        return hashCode;
    }
    
    @Override
    public EndQuorumEpochRequestData duplicate() {
        EndQuorumEpochRequestData _duplicate = new EndQuorumEpochRequestData();
        if (clusterId == null) {
            _duplicate.clusterId = null;
        } else {
            _duplicate.clusterId = clusterId;
        }
        ArrayList<TopicData> newTopics = new ArrayList<TopicData>(topics.size());
        for (TopicData _element : topics) {
            newTopics.add(_element.duplicate());
        }
        _duplicate.topics = newTopics;
        LeaderEndpointCollection newLeaderEndpoints = new LeaderEndpointCollection(leaderEndpoints.size());
        for (LeaderEndpoint _element : leaderEndpoints) {
            newLeaderEndpoints.add(_element.duplicate());
        }
        _duplicate.leaderEndpoints = newLeaderEndpoints;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "EndQuorumEpochRequestData("
            + "clusterId=" + ((clusterId == null) ? "null" : "'" + clusterId.toString() + "'")
            + ", topics=" + MessageUtil.deepToString(topics.iterator())
            + ", leaderEndpoints=" + MessageUtil.deepToString(leaderEndpoints.iterator())
            + ")";
    }
    
    public String clusterId() {
        return this.clusterId;
    }
    
    public List<TopicData> topics() {
        return this.topics;
    }
    
    public LeaderEndpointCollection leaderEndpoints() {
        return this.leaderEndpoints;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public EndQuorumEpochRequestData setClusterId(String v) {
        this.clusterId = v;
        return this;
    }
    
    public EndQuorumEpochRequestData setTopics(List<TopicData> v) {
        this.topics = v;
        return this;
    }
    
    public EndQuorumEpochRequestData setLeaderEndpoints(LeaderEndpointCollection v) {
        this.leaderEndpoints = v;
        return this;
    }
    
    public static class TopicData implements Message {
        String topicName;
        List<PartitionData> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic_name", Type.STRING, "The topic name."),
                new Field("partitions", new ArrayOf(PartitionData.SCHEMA_0), "The partitions.")
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("topic_name", Type.COMPACT_STRING, "The topic name."),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_1), "The partitions."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public TopicData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TopicData() {
            this.topicName = "";
            this.partitions = new ArrayList<PartitionData>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 1;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicData");
            }
            {
                int length;
                if (_version >= 1) {
                    length = _readable.readUnsignedVarint() - 1;
                } else {
                    length = _readable.readShort();
                }
                if (length < 0) {
                    throw new RuntimeException("non-nullable field topicName was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field topicName had invalid length " + length);
                } else {
                    this.topicName = _readable.readString(length);
                }
            }
            {
                if (_version >= 1) {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitions was serialized as null");
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<PartitionData> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new PartitionData(_readable, _version));
                        }
                        this.partitions = newCollection;
                    }
                } else {
                    int arrayLength;
                    arrayLength = _readable.readInt();
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitions was serialized as null");
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<PartitionData> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new PartitionData(_readable, _version));
                        }
                        this.partitions = newCollection;
                    }
                }
            }
            this._unknownTaggedFields = null;
            if (_version >= 1) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(topicName);
                if (_version >= 1) {
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                } else {
                    _writable.writeShort((short) _stringBytes.length);
                }
                _writable.writeByteArray(_stringBytes);
            }
            if (_version >= 1) {
                _writable.writeUnsignedVarint(partitions.size() + 1);
                for (PartitionData partitionsElement : partitions) {
                    partitionsElement.write(_writable, _cache, _version);
                }
            } else {
                _writable.writeInt(partitions.size());
                for (PartitionData partitionsElement : partitions) {
                    partitionsElement.write(_writable, _cache, _version);
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 1) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TopicData");
            }
            {
                byte[] _stringBytes = topicName.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'topicName' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(topicName, _stringBytes);
                if (_version >= 1) {
                    _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                } else {
                    _size.addBytes(_stringBytes.length + 2);
                }
            }
            {
                if (_version >= 1) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(partitions.size() + 1));
                } else {
                    _size.addBytes(4);
                }
                for (PartitionData partitionsElement : partitions) {
                    partitionsElement.addSize(_size, _cache, _version);
                }
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            if (_version >= 1) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicData)) return false;
            TopicData other = (TopicData) obj;
            if (this.topicName == null) {
                if (other.topicName != null) return false;
            } else {
                if (!this.topicName.equals(other.topicName)) return false;
            }
            if (this.partitions == null) {
                if (other.partitions != null) return false;
            } else {
                if (!this.partitions.equals(other.partitions)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topicName == null ? 0 : topicName.hashCode());
            hashCode = 31 * hashCode + (partitions == null ? 0 : partitions.hashCode());
            return hashCode;
        }
        
        @Override
        public TopicData duplicate() {
            TopicData _duplicate = new TopicData();
            _duplicate.topicName = topicName;
            ArrayList<PartitionData> newPartitions = new ArrayList<PartitionData>(partitions.size());
            for (PartitionData _element : partitions) {
                newPartitions.add(_element.duplicate());
            }
            _duplicate.partitions = newPartitions;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TopicData("
                + "topicName=" + ((topicName == null) ? "null" : "'" + topicName.toString() + "'")
                + ", partitions=" + MessageUtil.deepToString(partitions.iterator())
                + ")";
        }
        
        public String topicName() {
            return this.topicName;
        }
        
        public List<PartitionData> partitions() {
            return this.partitions;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TopicData setTopicName(String v) {
            this.topicName = v;
            return this;
        }
        
        public TopicData setPartitions(List<PartitionData> v) {
            this.partitions = v;
            return this;
        }
    }
    
    public static class PartitionData implements Message {
        int partitionIndex;
        int leaderId;
        int leaderEpoch;
        List<Integer> preferredSuccessors;
        List<ReplicaInfo> preferredCandidates;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("leader_id", Type.INT32, "The current leader ID that is resigning."),
                new Field("leader_epoch", Type.INT32, "The current epoch."),
                new Field("preferred_successors", new ArrayOf(Type.INT32), "A sorted list of preferred successors to start the election.")
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("leader_id", Type.INT32, "The current leader ID that is resigning."),
                new Field("leader_epoch", Type.INT32, "The current epoch."),
                new Field("preferred_candidates", new CompactArrayOf(ReplicaInfo.SCHEMA_1), "A sorted list of preferred candidates to start the election."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public PartitionData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionData() {
            this.partitionIndex = 0;
            this.leaderId = 0;
            this.leaderEpoch = 0;
            this.preferredSuccessors = new ArrayList<Integer>(0);
            this.preferredCandidates = new ArrayList<ReplicaInfo>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 1;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionData");
            }
            this.partitionIndex = _readable.readInt();
            this.leaderId = _readable.readInt();
            this.leaderEpoch = _readable.readInt();
            if (_version <= 0) {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field preferredSuccessors was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(_readable.readInt());
                    }
                    this.preferredSuccessors = newCollection;
                }
            } else {
                this.preferredSuccessors = new ArrayList<Integer>(0);
            }
            if (_version >= 1) {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field preferredCandidates was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<ReplicaInfo> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new ReplicaInfo(_readable, _version));
                    }
                    this.preferredCandidates = newCollection;
                }
            } else {
                this.preferredCandidates = new ArrayList<ReplicaInfo>(0);
            }
            this._unknownTaggedFields = null;
            if (_version >= 1) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(partitionIndex);
            _writable.writeInt(leaderId);
            _writable.writeInt(leaderEpoch);
            if (_version <= 0) {
                _writable.writeInt(preferredSuccessors.size());
                for (Integer preferredSuccessorsElement : preferredSuccessors) {
                    _writable.writeInt(preferredSuccessorsElement);
                }
            }
            if (_version >= 1) {
                _writable.writeUnsignedVarint(preferredCandidates.size() + 1);
                for (ReplicaInfo preferredCandidatesElement : preferredCandidates) {
                    preferredCandidatesElement.write(_writable, _cache, _version);
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 1) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionData");
            }
            _size.addBytes(4);
            _size.addBytes(4);
            _size.addBytes(4);
            if (_version <= 0) {
                {
                    _size.addBytes(4);
                    _size.addBytes(preferredSuccessors.size() * 4);
                }
            }
            if (_version >= 1) {
                {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(preferredCandidates.size() + 1));
                    for (ReplicaInfo preferredCandidatesElement : preferredCandidates) {
                        preferredCandidatesElement.addSize(_size, _cache, _version);
                    }
                }
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            if (_version >= 1) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionData)) return false;
            PartitionData other = (PartitionData) obj;
            if (partitionIndex != other.partitionIndex) return false;
            if (leaderId != other.leaderId) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            if (this.preferredSuccessors == null) {
                if (other.preferredSuccessors != null) return false;
            } else {
                if (!this.preferredSuccessors.equals(other.preferredSuccessors)) return false;
            }
            if (this.preferredCandidates == null) {
                if (other.preferredCandidates != null) return false;
            } else {
                if (!this.preferredCandidates.equals(other.preferredCandidates)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + leaderId;
            hashCode = 31 * hashCode + leaderEpoch;
            hashCode = 31 * hashCode + (preferredSuccessors == null ? 0 : preferredSuccessors.hashCode());
            hashCode = 31 * hashCode + (preferredCandidates == null ? 0 : preferredCandidates.hashCode());
            return hashCode;
        }
        
        @Override
        public PartitionData duplicate() {
            PartitionData _duplicate = new PartitionData();
            _duplicate.partitionIndex = partitionIndex;
            _duplicate.leaderId = leaderId;
            _duplicate.leaderEpoch = leaderEpoch;
            ArrayList<Integer> newPreferredSuccessors = new ArrayList<Integer>(preferredSuccessors.size());
            for (Integer _element : preferredSuccessors) {
                newPreferredSuccessors.add(_element);
            }
            _duplicate.preferredSuccessors = newPreferredSuccessors;
            ArrayList<ReplicaInfo> newPreferredCandidates = new ArrayList<ReplicaInfo>(preferredCandidates.size());
            for (ReplicaInfo _element : preferredCandidates) {
                newPreferredCandidates.add(_element.duplicate());
            }
            _duplicate.preferredCandidates = newPreferredCandidates;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionData("
                + "partitionIndex=" + partitionIndex
                + ", leaderId=" + leaderId
                + ", leaderEpoch=" + leaderEpoch
                + ", preferredSuccessors=" + MessageUtil.deepToString(preferredSuccessors.iterator())
                + ", preferredCandidates=" + MessageUtil.deepToString(preferredCandidates.iterator())
                + ")";
        }
        
        public int partitionIndex() {
            return this.partitionIndex;
        }
        
        public int leaderId() {
            return this.leaderId;
        }
        
        public int leaderEpoch() {
            return this.leaderEpoch;
        }
        
        public List<Integer> preferredSuccessors() {
            return this.preferredSuccessors;
        }
        
        public List<ReplicaInfo> preferredCandidates() {
            return this.preferredCandidates;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public PartitionData setPartitionIndex(int v) {
            this.partitionIndex = v;
            return this;
        }
        
        public PartitionData setLeaderId(int v) {
            this.leaderId = v;
            return this;
        }
        
        public PartitionData setLeaderEpoch(int v) {
            this.leaderEpoch = v;
            return this;
        }
        
        public PartitionData setPreferredSuccessors(List<Integer> v) {
            this.preferredSuccessors = v;
            return this;
        }
        
        public PartitionData setPreferredCandidates(List<ReplicaInfo> v) {
            this.preferredCandidates = v;
            return this;
        }
    }
    
    public static class ReplicaInfo implements Message {
        int candidateId;
        Uuid candidateDirectoryId;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("candidate_id", Type.INT32, "The ID of the candidate replica."),
                new Field("candidate_directory_id", Type.UUID, "The directory ID of the candidate replica."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 1;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public ReplicaInfo(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public ReplicaInfo() {
            this.candidateId = 0;
            this.candidateDirectoryId = Uuid.ZERO_UUID;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 1;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of ReplicaInfo");
            }
            this.candidateId = _readable.readInt();
            this.candidateDirectoryId = _readable.readUuid();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 1) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of ReplicaInfo");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(candidateId);
            _writable.writeUuid(candidateDirectoryId);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of ReplicaInfo");
            }
            _size.addBytes(4);
            _size.addBytes(16);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ReplicaInfo)) return false;
            ReplicaInfo other = (ReplicaInfo) obj;
            if (candidateId != other.candidateId) return false;
            if (!this.candidateDirectoryId.equals(other.candidateDirectoryId)) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + candidateId;
            hashCode = 31 * hashCode + candidateDirectoryId.hashCode();
            return hashCode;
        }
        
        @Override
        public ReplicaInfo duplicate() {
            ReplicaInfo _duplicate = new ReplicaInfo();
            _duplicate.candidateId = candidateId;
            _duplicate.candidateDirectoryId = candidateDirectoryId;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "ReplicaInfo("
                + "candidateId=" + candidateId
                + ", candidateDirectoryId=" + candidateDirectoryId.toString()
                + ")";
        }
        
        public int candidateId() {
            return this.candidateId;
        }
        
        public Uuid candidateDirectoryId() {
            return this.candidateDirectoryId;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public ReplicaInfo setCandidateId(int v) {
            this.candidateId = v;
            return this;
        }
        
        public ReplicaInfo setCandidateDirectoryId(Uuid v) {
            this.candidateDirectoryId = v;
            return this;
        }
    }
    
    public static class LeaderEndpoint implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        String host;
        int port;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the endpoint."),
                new Field("host", Type.COMPACT_STRING, "The node's hostname."),
                new Field("port", Type.UINT16, "The node's port."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 1;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public LeaderEndpoint(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public LeaderEndpoint() {
            this.name = "";
            this.host = "";
            this.port = 0;
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 1;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of LeaderEndpoint");
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field name was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field name had invalid length " + length);
                } else {
                    this.name = _readable.readString(length);
                }
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field host was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field host had invalid length " + length);
                } else {
                    this.host = _readable.readString(length);
                }
            }
            this.port = _readable.readUnsignedShort();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 1) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of LeaderEndpoint");
            }
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            {
                byte[] _stringBytes = _cache.getSerializedValue(host);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeUnsignedShort(port);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of LeaderEndpoint");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                byte[] _stringBytes = host.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'host' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(host, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            _size.addBytes(2);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof LeaderEndpoint)) return false;
            LeaderEndpoint other = (LeaderEndpoint) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LeaderEndpoint)) return false;
            LeaderEndpoint other = (LeaderEndpoint) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            if (this.host == null) {
                if (other.host != null) return false;
            } else {
                if (!this.host.equals(other.host)) return false;
            }
            if (port != other.port) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            return hashCode;
        }
        
        @Override
        public LeaderEndpoint duplicate() {
            LeaderEndpoint _duplicate = new LeaderEndpoint();
            _duplicate.name = name;
            _duplicate.host = host;
            _duplicate.port = port;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "LeaderEndpoint("
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", host=" + ((host == null) ? "null" : "'" + host.toString() + "'")
                + ", port=" + port
                + ")";
        }
        
        public String name() {
            return this.name;
        }
        
        public String host() {
            return this.host;
        }
        
        public int port() {
            return this.port;
        }
        
        @Override
        public int next() {
            return this.next;
        }
        
        @Override
        public int prev() {
            return this.prev;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public LeaderEndpoint setName(String v) {
            this.name = v;
            return this;
        }
        
        public LeaderEndpoint setHost(String v) {
            this.host = v;
            return this;
        }
        
        public LeaderEndpoint setPort(int v) {
            if (v < 0 || v > 65535) {
                throw new RuntimeException("Invalid value " + v + " for unsigned short field.");
            }
            this.port = v;
            return this;
        }
        
        @Override
        public void setNext(int v) {
            this.next = v;
        }
        
        @Override
        public void setPrev(int v) {
            this.prev = v;
        }
    }
    
    public static class LeaderEndpointCollection extends ImplicitLinkedHashMultiCollection<LeaderEndpoint> {
        public LeaderEndpointCollection() {
            super();
        }
        
        public LeaderEndpointCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public LeaderEndpointCollection(Iterator<LeaderEndpoint> iterator) {
            super(iterator);
        }
        
        public LeaderEndpoint find(String name) {
            LeaderEndpoint _key = new LeaderEndpoint();
            _key.setName(name);
            return find(_key);
        }
        
        public List<LeaderEndpoint> findAll(String name) {
            LeaderEndpoint _key = new LeaderEndpoint();
            _key.setName(name);
            return findAll(_key);
        }
        
        public LeaderEndpointCollection duplicate() {
            LeaderEndpointCollection _duplicate = new LeaderEndpointCollection(size());
            for (LeaderEndpoint _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
}
