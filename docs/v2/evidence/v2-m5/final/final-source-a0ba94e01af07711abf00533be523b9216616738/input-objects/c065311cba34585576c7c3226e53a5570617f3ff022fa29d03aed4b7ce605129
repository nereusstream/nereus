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
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class VoteRequestData implements ApiMessage {
    String clusterId;
    int voterId;
    List<TopicData> topics;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("cluster_id", Type.COMPACT_NULLABLE_STRING, "The cluster id."),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_0), "The topic data."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("cluster_id", Type.COMPACT_NULLABLE_STRING, "The cluster id."),
            new Field("voter_id", Type.INT32, "The replica id of the voter receiving the request."),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_1), "The topic data."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("cluster_id", Type.COMPACT_NULLABLE_STRING, "The cluster id."),
            new Field("voter_id", Type.INT32, "The replica id of the voter receiving the request."),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_2), "The topic data."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 2;
    
    public VoteRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public VoteRequestData() {
        this.clusterId = null;
        this.voterId = -1;
        this.topics = new ArrayList<TopicData>(0);
    }
    
    @Override
    public short apiKey() {
        return 52;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 2;
    }
    
    @Override
    public final void read(Readable _readable, short _version) {
        {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                this.clusterId = null;
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field clusterId had invalid length " + length);
            } else {
                this.clusterId = _readable.readString(length);
            }
        }
        if (_version >= 1) {
            this.voterId = _readable.readInt();
        } else {
            this.voterId = -1;
        }
        {
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
        }
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
        int _numTaggedFields = 0;
        if (clusterId == null) {
            _writable.writeUnsignedVarint(0);
        } else {
            byte[] _stringBytes = _cache.getSerializedValue(clusterId);
            _writable.writeUnsignedVarint(_stringBytes.length + 1);
            _writable.writeByteArray(_stringBytes);
        }
        if (_version >= 1) {
            _writable.writeInt(voterId);
        }
        _writable.writeUnsignedVarint(topics.size() + 1);
        for (TopicData topicsElement : topics) {
            topicsElement.write(_writable, _cache, _version);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (clusterId == null) {
            _size.addBytes(1);
        } else {
            byte[] _stringBytes = clusterId.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'clusterId' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(clusterId, _stringBytes);
            _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
        }
        if (_version >= 1) {
            _size.addBytes(4);
        }
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(topics.size() + 1));
            for (TopicData topicsElement : topics) {
                topicsElement.addSize(_size, _cache, _version);
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
        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VoteRequestData)) return false;
        VoteRequestData other = (VoteRequestData) obj;
        if (this.clusterId == null) {
            if (other.clusterId != null) return false;
        } else {
            if (!this.clusterId.equals(other.clusterId)) return false;
        }
        if (voterId != other.voterId) return false;
        if (this.topics == null) {
            if (other.topics != null) return false;
        } else {
            if (!this.topics.equals(other.topics)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (clusterId == null ? 0 : clusterId.hashCode());
        hashCode = 31 * hashCode + voterId;
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        return hashCode;
    }
    
    @Override
    public VoteRequestData duplicate() {
        VoteRequestData _duplicate = new VoteRequestData();
        if (clusterId == null) {
            _duplicate.clusterId = null;
        } else {
            _duplicate.clusterId = clusterId;
        }
        _duplicate.voterId = voterId;
        ArrayList<TopicData> newTopics = new ArrayList<TopicData>(topics.size());
        for (TopicData _element : topics) {
            newTopics.add(_element.duplicate());
        }
        _duplicate.topics = newTopics;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "VoteRequestData("
            + "clusterId=" + ((clusterId == null) ? "null" : "'" + clusterId.toString() + "'")
            + ", voterId=" + voterId
            + ", topics=" + MessageUtil.deepToString(topics.iterator())
            + ")";
    }
    
    public String clusterId() {
        return this.clusterId;
    }
    
    public int voterId() {
        return this.voterId;
    }
    
    public List<TopicData> topics() {
        return this.topics;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public VoteRequestData setClusterId(String v) {
        this.clusterId = v;
        return this;
    }
    
    public VoteRequestData setVoterId(int v) {
        this.voterId = v;
        return this;
    }
    
    public VoteRequestData setTopics(List<TopicData> v) {
        this.topics = v;
        return this;
    }
    
    public static class TopicData implements Message {
        String topicName;
        List<PartitionData> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic_name", Type.COMPACT_STRING, "The topic name."),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_0), "The partition data."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("topic_name", Type.COMPACT_STRING, "The topic name."),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_1), "The partition data."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("topic_name", Type.COMPACT_STRING, "The topic name."),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_2), "The partition data."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
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
            return 2;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicData");
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field topicName was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field topicName had invalid length " + length);
                } else {
                    this.topicName = _readable.readString(length);
                }
            }
            {
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
            }
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
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(topicName);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeUnsignedVarint(partitions.size() + 1);
            for (PartitionData partitionsElement : partitions) {
                partitionsElement.write(_writable, _cache, _version);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TopicData");
            }
            {
                byte[] _stringBytes = topicName.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'topicName' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(topicName, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(partitions.size() + 1));
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
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
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
        int replicaEpoch;
        int replicaId;
        Uuid replicaDirectoryId;
        Uuid voterDirectoryId;
        int lastOffsetEpoch;
        long lastOffset;
        boolean preVote;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("replica_epoch", Type.INT32, "The epoch of the voter sending the request"),
                new Field("replica_id", Type.INT32, "The replica id of the voter sending the request"),
                new Field("last_offset_epoch", Type.INT32, "The epoch of the last record written to the metadata log."),
                new Field("last_offset", Type.INT64, "The log end offset of the metadata log of the voter sending the request."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("replica_epoch", Type.INT32, "The epoch of the voter sending the request"),
                new Field("replica_id", Type.INT32, "The replica id of the voter sending the request"),
                new Field("replica_directory_id", Type.UUID, "The directory id of the voter sending the request"),
                new Field("voter_directory_id", Type.UUID, "The directory id of the voter receiving the request"),
                new Field("last_offset_epoch", Type.INT32, "The epoch of the last record written to the metadata log."),
                new Field("last_offset", Type.INT64, "The log end offset of the metadata log of the voter sending the request."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("replica_epoch", Type.INT32, "The epoch of the voter sending the request"),
                new Field("replica_id", Type.INT32, "The replica id of the voter sending the request"),
                new Field("replica_directory_id", Type.UUID, "The directory id of the voter sending the request"),
                new Field("voter_directory_id", Type.UUID, "The directory id of the voter receiving the request"),
                new Field("last_offset_epoch", Type.INT32, "The epoch of the last record written to the metadata log."),
                new Field("last_offset", Type.INT64, "The log end offset of the metadata log of the voter sending the request."),
                new Field("pre_vote", Type.BOOLEAN, "Whether the request is a PreVote request (not persisted) or not."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public PartitionData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionData() {
            this.partitionIndex = 0;
            this.replicaEpoch = 0;
            this.replicaId = 0;
            this.replicaDirectoryId = Uuid.ZERO_UUID;
            this.voterDirectoryId = Uuid.ZERO_UUID;
            this.lastOffsetEpoch = 0;
            this.lastOffset = 0L;
            this.preVote = false;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 2;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionData");
            }
            this.partitionIndex = _readable.readInt();
            this.replicaEpoch = _readable.readInt();
            this.replicaId = _readable.readInt();
            if (_version >= 1) {
                this.replicaDirectoryId = _readable.readUuid();
            } else {
                this.replicaDirectoryId = Uuid.ZERO_UUID;
            }
            if (_version >= 1) {
                this.voterDirectoryId = _readable.readUuid();
            } else {
                this.voterDirectoryId = Uuid.ZERO_UUID;
            }
            this.lastOffsetEpoch = _readable.readInt();
            this.lastOffset = _readable.readLong();
            if (_version >= 2) {
                this.preVote = _readable.readByte() != 0;
            } else {
                this.preVote = false;
            }
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
            int _numTaggedFields = 0;
            _writable.writeInt(partitionIndex);
            _writable.writeInt(replicaEpoch);
            _writable.writeInt(replicaId);
            if (_version >= 1) {
                _writable.writeUuid(replicaDirectoryId);
            }
            if (_version >= 1) {
                _writable.writeUuid(voterDirectoryId);
            }
            _writable.writeInt(lastOffsetEpoch);
            _writable.writeLong(lastOffset);
            if (_version >= 2) {
                _writable.writeByte(preVote ? (byte) 1 : (byte) 0);
            } else {
                if (this.preVote) {
                    throw new UnsupportedVersionException("Attempted to write a non-default preVote at version " + _version);
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionData");
            }
            _size.addBytes(4);
            _size.addBytes(4);
            _size.addBytes(4);
            if (_version >= 1) {
                _size.addBytes(16);
            }
            if (_version >= 1) {
                _size.addBytes(16);
            }
            _size.addBytes(4);
            _size.addBytes(8);
            if (_version >= 2) {
                _size.addBytes(1);
            }
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
            if (!(obj instanceof PartitionData)) return false;
            PartitionData other = (PartitionData) obj;
            if (partitionIndex != other.partitionIndex) return false;
            if (replicaEpoch != other.replicaEpoch) return false;
            if (replicaId != other.replicaId) return false;
            if (!this.replicaDirectoryId.equals(other.replicaDirectoryId)) return false;
            if (!this.voterDirectoryId.equals(other.voterDirectoryId)) return false;
            if (lastOffsetEpoch != other.lastOffsetEpoch) return false;
            if (lastOffset != other.lastOffset) return false;
            if (preVote != other.preVote) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + replicaEpoch;
            hashCode = 31 * hashCode + replicaId;
            hashCode = 31 * hashCode + replicaDirectoryId.hashCode();
            hashCode = 31 * hashCode + voterDirectoryId.hashCode();
            hashCode = 31 * hashCode + lastOffsetEpoch;
            hashCode = 31 * hashCode + ((int) (lastOffset >> 32) ^ (int) lastOffset);
            hashCode = 31 * hashCode + (preVote ? 1231 : 1237);
            return hashCode;
        }
        
        @Override
        public PartitionData duplicate() {
            PartitionData _duplicate = new PartitionData();
            _duplicate.partitionIndex = partitionIndex;
            _duplicate.replicaEpoch = replicaEpoch;
            _duplicate.replicaId = replicaId;
            _duplicate.replicaDirectoryId = replicaDirectoryId;
            _duplicate.voterDirectoryId = voterDirectoryId;
            _duplicate.lastOffsetEpoch = lastOffsetEpoch;
            _duplicate.lastOffset = lastOffset;
            _duplicate.preVote = preVote;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionData("
                + "partitionIndex=" + partitionIndex
                + ", replicaEpoch=" + replicaEpoch
                + ", replicaId=" + replicaId
                + ", replicaDirectoryId=" + replicaDirectoryId.toString()
                + ", voterDirectoryId=" + voterDirectoryId.toString()
                + ", lastOffsetEpoch=" + lastOffsetEpoch
                + ", lastOffset=" + lastOffset
                + ", preVote=" + (preVote ? "true" : "false")
                + ")";
        }
        
        public int partitionIndex() {
            return this.partitionIndex;
        }
        
        public int replicaEpoch() {
            return this.replicaEpoch;
        }
        
        public int replicaId() {
            return this.replicaId;
        }
        
        public Uuid replicaDirectoryId() {
            return this.replicaDirectoryId;
        }
        
        public Uuid voterDirectoryId() {
            return this.voterDirectoryId;
        }
        
        public int lastOffsetEpoch() {
            return this.lastOffsetEpoch;
        }
        
        public long lastOffset() {
            return this.lastOffset;
        }
        
        public boolean preVote() {
            return this.preVote;
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
        
        public PartitionData setReplicaEpoch(int v) {
            this.replicaEpoch = v;
            return this;
        }
        
        public PartitionData setReplicaId(int v) {
            this.replicaId = v;
            return this;
        }
        
        public PartitionData setReplicaDirectoryId(Uuid v) {
            this.replicaDirectoryId = v;
            return this;
        }
        
        public PartitionData setVoterDirectoryId(Uuid v) {
            this.voterDirectoryId = v;
            return this;
        }
        
        public PartitionData setLastOffsetEpoch(int v) {
            this.lastOffsetEpoch = v;
            return this;
        }
        
        public PartitionData setLastOffset(long v) {
            this.lastOffset = v;
            return this;
        }
        
        public PartitionData setPreVote(boolean v) {
            this.preVote = v;
            return this;
        }
    }
}
