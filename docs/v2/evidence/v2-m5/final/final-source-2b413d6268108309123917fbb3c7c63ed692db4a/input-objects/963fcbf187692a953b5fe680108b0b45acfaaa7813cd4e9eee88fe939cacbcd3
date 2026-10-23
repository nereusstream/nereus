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


public class DescribeQuorumResponseData implements ApiMessage {
    short errorCode;
    String errorMessage;
    List<TopicData> topics;
    NodeCollection nodes;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("error_code", Type.INT16, "The top level error code."),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_0), "The response from the describe quorum API."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("error_code", Type.INT16, "The top level error code."),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_1), "The response from the describe quorum API."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("error_code", Type.INT16, "The top level error code."),
            new Field("error_message", Type.COMPACT_NULLABLE_STRING, "The error message, or null if there was no error."),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_2), "The response from the describe quorum API."),
            new Field("nodes", new CompactArrayOf(Node.SCHEMA_2), "The nodes in the quorum."),
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
    
    public DescribeQuorumResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public DescribeQuorumResponseData() {
        this.errorCode = (short) 0;
        this.errorMessage = "";
        this.topics = new ArrayList<TopicData>(0);
        this.nodes = new NodeCollection(0);
    }
    
    @Override
    public short apiKey() {
        return 55;
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
        this.errorCode = _readable.readShort();
        if (_version >= 2) {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                this.errorMessage = null;
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field errorMessage had invalid length " + length);
            } else {
                this.errorMessage = _readable.readString(length);
            }
        } else {
            this.errorMessage = "";
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
        if (_version >= 2) {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field nodes was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                NodeCollection newCollection = new NodeCollection(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new Node(_readable, _version));
                }
                this.nodes = newCollection;
            }
        } else {
            this.nodes = new NodeCollection(0);
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
        _writable.writeShort(errorCode);
        if (_version >= 2) {
            if (errorMessage == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(errorMessage);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
        }
        _writable.writeUnsignedVarint(topics.size() + 1);
        for (TopicData topicsElement : topics) {
            topicsElement.write(_writable, _cache, _version);
        }
        if (_version >= 2) {
            _writable.writeUnsignedVarint(nodes.size() + 1);
            for (Node nodesElement : nodes) {
                nodesElement.write(_writable, _cache, _version);
            }
        } else {
            if (!this.nodes.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default nodes at version " + _version);
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
        _size.addBytes(2);
        if (_version >= 2) {
            if (errorMessage == null) {
                _size.addBytes(1);
            } else {
                byte[] _stringBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'errorMessage' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(errorMessage, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
        }
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(topics.size() + 1));
            for (TopicData topicsElement : topics) {
                topicsElement.addSize(_size, _cache, _version);
            }
        }
        if (_version >= 2) {
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(nodes.size() + 1));
                for (Node nodesElement : nodes) {
                    nodesElement.addSize(_size, _cache, _version);
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
        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DescribeQuorumResponseData)) return false;
        DescribeQuorumResponseData other = (DescribeQuorumResponseData) obj;
        if (errorCode != other.errorCode) return false;
        if (this.errorMessage == null) {
            if (other.errorMessage != null) return false;
        } else {
            if (!this.errorMessage.equals(other.errorMessage)) return false;
        }
        if (this.topics == null) {
            if (other.topics != null) return false;
        } else {
            if (!this.topics.equals(other.topics)) return false;
        }
        if (this.nodes == null) {
            if (other.nodes != null) return false;
        } else {
            if (!this.nodes.equals(other.nodes)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + (errorMessage == null ? 0 : errorMessage.hashCode());
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        hashCode = 31 * hashCode + (nodes == null ? 0 : nodes.hashCode());
        return hashCode;
    }
    
    @Override
    public DescribeQuorumResponseData duplicate() {
        DescribeQuorumResponseData _duplicate = new DescribeQuorumResponseData();
        _duplicate.errorCode = errorCode;
        if (errorMessage == null) {
            _duplicate.errorMessage = null;
        } else {
            _duplicate.errorMessage = errorMessage;
        }
        ArrayList<TopicData> newTopics = new ArrayList<TopicData>(topics.size());
        for (TopicData _element : topics) {
            newTopics.add(_element.duplicate());
        }
        _duplicate.topics = newTopics;
        NodeCollection newNodes = new NodeCollection(nodes.size());
        for (Node _element : nodes) {
            newNodes.add(_element.duplicate());
        }
        _duplicate.nodes = newNodes;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "DescribeQuorumResponseData("
            + "errorCode=" + errorCode
            + ", errorMessage=" + ((errorMessage == null) ? "null" : "'" + errorMessage.toString() + "'")
            + ", topics=" + MessageUtil.deepToString(topics.iterator())
            + ", nodes=" + MessageUtil.deepToString(nodes.iterator())
            + ")";
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public String errorMessage() {
        return this.errorMessage;
    }
    
    public List<TopicData> topics() {
        return this.topics;
    }
    
    public NodeCollection nodes() {
        return this.nodes;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public DescribeQuorumResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public DescribeQuorumResponseData setErrorMessage(String v) {
        this.errorMessage = v;
        return this;
    }
    
    public DescribeQuorumResponseData setTopics(List<TopicData> v) {
        this.topics = v;
        return this;
    }
    
    public DescribeQuorumResponseData setNodes(NodeCollection v) {
        this.nodes = v;
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
        short errorCode;
        String errorMessage;
        int leaderId;
        int leaderEpoch;
        long highWatermark;
        List<ReplicaState> currentVoters;
        List<ReplicaState> observers;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The partition error code."),
                new Field("leader_id", Type.INT32, "The ID of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch."),
                new Field("high_watermark", Type.INT64, "The high water mark."),
                new Field("current_voters", new CompactArrayOf(ReplicaState.SCHEMA_0), "The current voters of the partition."),
                new Field("observers", new CompactArrayOf(ReplicaState.SCHEMA_0), "The observers of the partition."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The partition error code."),
                new Field("leader_id", Type.INT32, "The ID of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch."),
                new Field("high_watermark", Type.INT64, "The high water mark."),
                new Field("current_voters", new CompactArrayOf(ReplicaState.SCHEMA_1), "The current voters of the partition."),
                new Field("observers", new CompactArrayOf(ReplicaState.SCHEMA_1), "The observers of the partition."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The partition error code."),
                new Field("error_message", Type.COMPACT_NULLABLE_STRING, "The error message, or null if there was no error."),
                new Field("leader_id", Type.INT32, "The ID of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch."),
                new Field("high_watermark", Type.INT64, "The high water mark."),
                new Field("current_voters", new CompactArrayOf(ReplicaState.SCHEMA_2), "The current voters of the partition."),
                new Field("observers", new CompactArrayOf(ReplicaState.SCHEMA_2), "The observers of the partition."),
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
            this.errorCode = (short) 0;
            this.errorMessage = "";
            this.leaderId = 0;
            this.leaderEpoch = 0;
            this.highWatermark = 0L;
            this.currentVoters = new ArrayList<ReplicaState>(0);
            this.observers = new ArrayList<ReplicaState>(0);
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
            this.errorCode = _readable.readShort();
            if (_version >= 2) {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    this.errorMessage = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field errorMessage had invalid length " + length);
                } else {
                    this.errorMessage = _readable.readString(length);
                }
            } else {
                this.errorMessage = "";
            }
            this.leaderId = _readable.readInt();
            this.leaderEpoch = _readable.readInt();
            this.highWatermark = _readable.readLong();
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field currentVoters was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<ReplicaState> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new ReplicaState(_readable, _version));
                    }
                    this.currentVoters = newCollection;
                }
            }
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field observers was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<ReplicaState> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new ReplicaState(_readable, _version));
                    }
                    this.observers = newCollection;
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
            _writable.writeInt(partitionIndex);
            _writable.writeShort(errorCode);
            if (_version >= 2) {
                if (errorMessage == null) {
                    _writable.writeUnsignedVarint(0);
                } else {
                    byte[] _stringBytes = _cache.getSerializedValue(errorMessage);
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                    _writable.writeByteArray(_stringBytes);
                }
            }
            _writable.writeInt(leaderId);
            _writable.writeInt(leaderEpoch);
            _writable.writeLong(highWatermark);
            _writable.writeUnsignedVarint(currentVoters.size() + 1);
            for (ReplicaState currentVotersElement : currentVoters) {
                currentVotersElement.write(_writable, _cache, _version);
            }
            _writable.writeUnsignedVarint(observers.size() + 1);
            for (ReplicaState observersElement : observers) {
                observersElement.write(_writable, _cache, _version);
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
            _size.addBytes(2);
            if (_version >= 2) {
                if (errorMessage == null) {
                    _size.addBytes(1);
                } else {
                    byte[] _stringBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
                    if (_stringBytes.length > 0x7fff) {
                        throw new RuntimeException("'errorMessage' field is too long to be serialized");
                    }
                    _cache.cacheSerializedValue(errorMessage, _stringBytes);
                    _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                }
            }
            _size.addBytes(4);
            _size.addBytes(4);
            _size.addBytes(8);
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(currentVoters.size() + 1));
                for (ReplicaState currentVotersElement : currentVoters) {
                    currentVotersElement.addSize(_size, _cache, _version);
                }
            }
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(observers.size() + 1));
                for (ReplicaState observersElement : observers) {
                    observersElement.addSize(_size, _cache, _version);
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
            if (!(obj instanceof PartitionData)) return false;
            PartitionData other = (PartitionData) obj;
            if (partitionIndex != other.partitionIndex) return false;
            if (errorCode != other.errorCode) return false;
            if (this.errorMessage == null) {
                if (other.errorMessage != null) return false;
            } else {
                if (!this.errorMessage.equals(other.errorMessage)) return false;
            }
            if (leaderId != other.leaderId) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            if (highWatermark != other.highWatermark) return false;
            if (this.currentVoters == null) {
                if (other.currentVoters != null) return false;
            } else {
                if (!this.currentVoters.equals(other.currentVoters)) return false;
            }
            if (this.observers == null) {
                if (other.observers != null) return false;
            } else {
                if (!this.observers.equals(other.observers)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + errorCode;
            hashCode = 31 * hashCode + (errorMessage == null ? 0 : errorMessage.hashCode());
            hashCode = 31 * hashCode + leaderId;
            hashCode = 31 * hashCode + leaderEpoch;
            hashCode = 31 * hashCode + ((int) (highWatermark >> 32) ^ (int) highWatermark);
            hashCode = 31 * hashCode + (currentVoters == null ? 0 : currentVoters.hashCode());
            hashCode = 31 * hashCode + (observers == null ? 0 : observers.hashCode());
            return hashCode;
        }
        
        @Override
        public PartitionData duplicate() {
            PartitionData _duplicate = new PartitionData();
            _duplicate.partitionIndex = partitionIndex;
            _duplicate.errorCode = errorCode;
            if (errorMessage == null) {
                _duplicate.errorMessage = null;
            } else {
                _duplicate.errorMessage = errorMessage;
            }
            _duplicate.leaderId = leaderId;
            _duplicate.leaderEpoch = leaderEpoch;
            _duplicate.highWatermark = highWatermark;
            ArrayList<ReplicaState> newCurrentVoters = new ArrayList<ReplicaState>(currentVoters.size());
            for (ReplicaState _element : currentVoters) {
                newCurrentVoters.add(_element.duplicate());
            }
            _duplicate.currentVoters = newCurrentVoters;
            ArrayList<ReplicaState> newObservers = new ArrayList<ReplicaState>(observers.size());
            for (ReplicaState _element : observers) {
                newObservers.add(_element.duplicate());
            }
            _duplicate.observers = newObservers;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionData("
                + "partitionIndex=" + partitionIndex
                + ", errorCode=" + errorCode
                + ", errorMessage=" + ((errorMessage == null) ? "null" : "'" + errorMessage.toString() + "'")
                + ", leaderId=" + leaderId
                + ", leaderEpoch=" + leaderEpoch
                + ", highWatermark=" + highWatermark
                + ", currentVoters=" + MessageUtil.deepToString(currentVoters.iterator())
                + ", observers=" + MessageUtil.deepToString(observers.iterator())
                + ")";
        }
        
        public int partitionIndex() {
            return this.partitionIndex;
        }
        
        public short errorCode() {
            return this.errorCode;
        }
        
        public String errorMessage() {
            return this.errorMessage;
        }
        
        public int leaderId() {
            return this.leaderId;
        }
        
        public int leaderEpoch() {
            return this.leaderEpoch;
        }
        
        public long highWatermark() {
            return this.highWatermark;
        }
        
        public List<ReplicaState> currentVoters() {
            return this.currentVoters;
        }
        
        public List<ReplicaState> observers() {
            return this.observers;
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
        
        public PartitionData setErrorCode(short v) {
            this.errorCode = v;
            return this;
        }
        
        public PartitionData setErrorMessage(String v) {
            this.errorMessage = v;
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
        
        public PartitionData setHighWatermark(long v) {
            this.highWatermark = v;
            return this;
        }
        
        public PartitionData setCurrentVoters(List<ReplicaState> v) {
            this.currentVoters = v;
            return this;
        }
        
        public PartitionData setObservers(List<ReplicaState> v) {
            this.observers = v;
            return this;
        }
    }
    
    public static class Node implements Message, ImplicitLinkedHashMultiCollection.Element {
        int nodeId;
        ListenerCollection listeners;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("node_id", Type.INT32, "The ID of the associated node."),
                new Field("listeners", new CompactArrayOf(Listener.SCHEMA_2), "The listeners of this controller."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 2;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public Node(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public Node() {
            this.nodeId = 0;
            this.listeners = new ListenerCollection(0);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of Node");
            }
            this.nodeId = _readable.readInt();
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field listeners was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ListenerCollection newCollection = new ListenerCollection(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new Listener(_readable, _version));
                    }
                    this.listeners = newCollection;
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
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of Node");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(nodeId);
            _writable.writeUnsignedVarint(listeners.size() + 1);
            for (Listener listenersElement : listeners) {
                listenersElement.write(_writable, _cache, _version);
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
                throw new UnsupportedVersionException("Can't size version " + _version + " of Node");
            }
            _size.addBytes(4);
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(listeners.size() + 1));
                for (Listener listenersElement : listeners) {
                    listenersElement.addSize(_size, _cache, _version);
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
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof Node)) return false;
            Node other = (Node) obj;
            if (nodeId != other.nodeId) return false;
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Node)) return false;
            Node other = (Node) obj;
            if (nodeId != other.nodeId) return false;
            if (this.listeners == null) {
                if (other.listeners != null) return false;
            } else {
                if (!this.listeners.equals(other.listeners)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + nodeId;
            return hashCode;
        }
        
        @Override
        public Node duplicate() {
            Node _duplicate = new Node();
            _duplicate.nodeId = nodeId;
            ListenerCollection newListeners = new ListenerCollection(listeners.size());
            for (Listener _element : listeners) {
                newListeners.add(_element.duplicate());
            }
            _duplicate.listeners = newListeners;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Node("
                + "nodeId=" + nodeId
                + ", listeners=" + MessageUtil.deepToString(listeners.iterator())
                + ")";
        }
        
        public int nodeId() {
            return this.nodeId;
        }
        
        public ListenerCollection listeners() {
            return this.listeners;
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
        
        public Node setNodeId(int v) {
            this.nodeId = v;
            return this;
        }
        
        public Node setListeners(ListenerCollection v) {
            this.listeners = v;
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
    
    public static class Listener implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        String host;
        int port;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the endpoint."),
                new Field("host", Type.COMPACT_STRING, "The hostname."),
                new Field("port", Type.UINT16, "The port."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            SCHEMA_2
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 2;
        public static final short HIGHEST_SUPPORTED_VERSION = 2;
        
        public Listener(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public Listener() {
            this.name = "";
            this.host = "";
            this.port = 0;
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 2;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 2;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of Listener");
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
            if (_version > 2) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of Listener");
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
            if (!(obj instanceof Listener)) return false;
            Listener other = (Listener) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Listener)) return false;
            Listener other = (Listener) obj;
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
        public Listener duplicate() {
            Listener _duplicate = new Listener();
            _duplicate.name = name;
            _duplicate.host = host;
            _duplicate.port = port;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Listener("
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
        
        public Listener setName(String v) {
            this.name = v;
            return this;
        }
        
        public Listener setHost(String v) {
            this.host = v;
            return this;
        }
        
        public Listener setPort(int v) {
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
    
    public static class ListenerCollection extends ImplicitLinkedHashMultiCollection<Listener> {
        public ListenerCollection() {
            super();
        }
        
        public ListenerCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public ListenerCollection(Iterator<Listener> iterator) {
            super(iterator);
        }
        
        public Listener find(String name) {
            Listener _key = new Listener();
            _key.setName(name);
            return find(_key);
        }
        
        public List<Listener> findAll(String name) {
            Listener _key = new Listener();
            _key.setName(name);
            return findAll(_key);
        }
        
        public ListenerCollection duplicate() {
            ListenerCollection _duplicate = new ListenerCollection(size());
            for (Listener _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
    
    public static class NodeCollection extends ImplicitLinkedHashMultiCollection<Node> {
        public NodeCollection() {
            super();
        }
        
        public NodeCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public NodeCollection(Iterator<Node> iterator) {
            super(iterator);
        }
        
        public Node find(int nodeId) {
            Node _key = new Node();
            _key.setNodeId(nodeId);
            return find(_key);
        }
        
        public List<Node> findAll(int nodeId) {
            Node _key = new Node();
            _key.setNodeId(nodeId);
            return findAll(_key);
        }
        
        public NodeCollection duplicate() {
            NodeCollection _duplicate = new NodeCollection(size());
            for (Node _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
    
    public static class ReplicaState implements Message {
        int replicaId;
        Uuid replicaDirectoryId;
        long logEndOffset;
        long lastFetchTimestamp;
        long lastCaughtUpTimestamp;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("replica_id", Type.INT32, "The ID of the replica."),
                new Field("log_end_offset", Type.INT64, "The last known log end offset of the follower or -1 if it is unknown."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("replica_id", Type.INT32, "The ID of the replica."),
                new Field("log_end_offset", Type.INT64, "The last known log end offset of the follower or -1 if it is unknown."),
                new Field("last_fetch_timestamp", Type.INT64, "The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter."),
                new Field("last_caught_up_timestamp", Type.INT64, "The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_2 =
            new Schema(
                new Field("replica_id", Type.INT32, "The ID of the replica."),
                new Field("replica_directory_id", Type.UUID, "The replica directory ID of the replica."),
                new Field("log_end_offset", Type.INT64, "The last known log end offset of the follower or -1 if it is unknown."),
                new Field("last_fetch_timestamp", Type.INT64, "The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter."),
                new Field("last_caught_up_timestamp", Type.INT64, "The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter."),
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
        
        public ReplicaState(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public ReplicaState() {
            this.replicaId = 0;
            this.replicaDirectoryId = Uuid.ZERO_UUID;
            this.logEndOffset = 0L;
            this.lastFetchTimestamp = -1L;
            this.lastCaughtUpTimestamp = -1L;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 32767;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            this.replicaId = _readable.readInt();
            if (_version >= 2) {
                this.replicaDirectoryId = _readable.readUuid();
            } else {
                this.replicaDirectoryId = Uuid.ZERO_UUID;
            }
            this.logEndOffset = _readable.readLong();
            if (_version >= 1) {
                this.lastFetchTimestamp = _readable.readLong();
            } else {
                this.lastFetchTimestamp = -1L;
            }
            if (_version >= 1) {
                this.lastCaughtUpTimestamp = _readable.readLong();
            } else {
                this.lastCaughtUpTimestamp = -1L;
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
            _writable.writeInt(replicaId);
            if (_version >= 2) {
                _writable.writeUuid(replicaDirectoryId);
            } else {
                if (!this.replicaDirectoryId.equals(Uuid.ZERO_UUID)) {
                    throw new UnsupportedVersionException("Attempted to write a non-default replicaDirectoryId at version " + _version);
                }
            }
            _writable.writeLong(logEndOffset);
            if (_version >= 1) {
                _writable.writeLong(lastFetchTimestamp);
            }
            if (_version >= 1) {
                _writable.writeLong(lastCaughtUpTimestamp);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _size.addBytes(4);
            if (_version >= 2) {
                _size.addBytes(16);
            }
            _size.addBytes(8);
            if (_version >= 1) {
                _size.addBytes(8);
            }
            if (_version >= 1) {
                _size.addBytes(8);
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
            if (!(obj instanceof ReplicaState)) return false;
            ReplicaState other = (ReplicaState) obj;
            if (replicaId != other.replicaId) return false;
            if (!this.replicaDirectoryId.equals(other.replicaDirectoryId)) return false;
            if (logEndOffset != other.logEndOffset) return false;
            if (lastFetchTimestamp != other.lastFetchTimestamp) return false;
            if (lastCaughtUpTimestamp != other.lastCaughtUpTimestamp) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + replicaId;
            hashCode = 31 * hashCode + replicaDirectoryId.hashCode();
            hashCode = 31 * hashCode + ((int) (logEndOffset >> 32) ^ (int) logEndOffset);
            hashCode = 31 * hashCode + ((int) (lastFetchTimestamp >> 32) ^ (int) lastFetchTimestamp);
            hashCode = 31 * hashCode + ((int) (lastCaughtUpTimestamp >> 32) ^ (int) lastCaughtUpTimestamp);
            return hashCode;
        }
        
        @Override
        public ReplicaState duplicate() {
            ReplicaState _duplicate = new ReplicaState();
            _duplicate.replicaId = replicaId;
            _duplicate.replicaDirectoryId = replicaDirectoryId;
            _duplicate.logEndOffset = logEndOffset;
            _duplicate.lastFetchTimestamp = lastFetchTimestamp;
            _duplicate.lastCaughtUpTimestamp = lastCaughtUpTimestamp;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "ReplicaState("
                + "replicaId=" + replicaId
                + ", replicaDirectoryId=" + replicaDirectoryId.toString()
                + ", logEndOffset=" + logEndOffset
                + ", lastFetchTimestamp=" + lastFetchTimestamp
                + ", lastCaughtUpTimestamp=" + lastCaughtUpTimestamp
                + ")";
        }
        
        public int replicaId() {
            return this.replicaId;
        }
        
        public Uuid replicaDirectoryId() {
            return this.replicaDirectoryId;
        }
        
        public long logEndOffset() {
            return this.logEndOffset;
        }
        
        public long lastFetchTimestamp() {
            return this.lastFetchTimestamp;
        }
        
        public long lastCaughtUpTimestamp() {
            return this.lastCaughtUpTimestamp;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public ReplicaState setReplicaId(int v) {
            this.replicaId = v;
            return this;
        }
        
        public ReplicaState setReplicaDirectoryId(Uuid v) {
            this.replicaDirectoryId = v;
            return this;
        }
        
        public ReplicaState setLogEndOffset(long v) {
            this.logEndOffset = v;
            return this;
        }
        
        public ReplicaState setLastFetchTimestamp(long v) {
            this.lastFetchTimestamp = v;
            return this;
        }
        
        public ReplicaState setLastCaughtUpTimestamp(long v) {
            this.lastCaughtUpTimestamp = v;
            return this;
        }
    }
}
