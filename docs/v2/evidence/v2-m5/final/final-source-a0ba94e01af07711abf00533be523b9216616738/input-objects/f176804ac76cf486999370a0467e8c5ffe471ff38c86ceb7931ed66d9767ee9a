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


public class ConsumerGroupDescribeResponseData implements ApiMessage {
    int throttleTimeMs;
    List<DescribedGroup> groups;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("groups", new CompactArrayOf(DescribedGroup.SCHEMA_0), "Each described group."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("groups", new CompactArrayOf(DescribedGroup.SCHEMA_1), "Each described group."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 1;
    
    public ConsumerGroupDescribeResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public ConsumerGroupDescribeResponseData() {
        this.throttleTimeMs = 0;
        this.groups = new ArrayList<DescribedGroup>(0);
    }
    
    @Override
    public short apiKey() {
        return 69;
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
        this.throttleTimeMs = _readable.readInt();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field groups was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ArrayList<DescribedGroup> newCollection = new ArrayList<>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new DescribedGroup(_readable, _version));
                }
                this.groups = newCollection;
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
        _writable.writeInt(throttleTimeMs);
        _writable.writeUnsignedVarint(groups.size() + 1);
        for (DescribedGroup groupsElement : groups) {
            groupsElement.write(_writable, _cache, _version);
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
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(groups.size() + 1));
            for (DescribedGroup groupsElement : groups) {
                groupsElement.addSize(_size, _cache, _version);
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
        if (!(obj instanceof ConsumerGroupDescribeResponseData)) return false;
        ConsumerGroupDescribeResponseData other = (ConsumerGroupDescribeResponseData) obj;
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (this.groups == null) {
            if (other.groups != null) return false;
        } else {
            if (!this.groups.equals(other.groups)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + (groups == null ? 0 : groups.hashCode());
        return hashCode;
    }
    
    @Override
    public ConsumerGroupDescribeResponseData duplicate() {
        ConsumerGroupDescribeResponseData _duplicate = new ConsumerGroupDescribeResponseData();
        _duplicate.throttleTimeMs = throttleTimeMs;
        ArrayList<DescribedGroup> newGroups = new ArrayList<DescribedGroup>(groups.size());
        for (DescribedGroup _element : groups) {
            newGroups.add(_element.duplicate());
        }
        _duplicate.groups = newGroups;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "ConsumerGroupDescribeResponseData("
            + "throttleTimeMs=" + throttleTimeMs
            + ", groups=" + MessageUtil.deepToString(groups.iterator())
            + ")";
    }
    
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    public List<DescribedGroup> groups() {
        return this.groups;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public ConsumerGroupDescribeResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }
    
    public ConsumerGroupDescribeResponseData setGroups(List<DescribedGroup> v) {
        this.groups = v;
        return this;
    }
    
    public static class DescribedGroup implements Message {
        short errorCode;
        String errorMessage;
        String groupId;
        String groupState;
        int groupEpoch;
        int assignmentEpoch;
        String assignorName;
        List<Member> members;
        int authorizedOperations;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("error_code", Type.INT16, "The describe error, or 0 if there was no error."),
                new Field("error_message", Type.COMPACT_NULLABLE_STRING, "The top-level error message, or null if there was no error."),
                new Field("group_id", Type.COMPACT_STRING, "The group ID string."),
                new Field("group_state", Type.COMPACT_STRING, "The group state string, or the empty string."),
                new Field("group_epoch", Type.INT32, "The group epoch."),
                new Field("assignment_epoch", Type.INT32, "The assignment epoch."),
                new Field("assignor_name", Type.COMPACT_STRING, "The selected assignor."),
                new Field("members", new CompactArrayOf(Member.SCHEMA_0), "The members."),
                new Field("authorized_operations", Type.INT32, "32-bit bitfield to represent authorized operations for this group."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("error_code", Type.INT16, "The describe error, or 0 if there was no error."),
                new Field("error_message", Type.COMPACT_NULLABLE_STRING, "The top-level error message, or null if there was no error."),
                new Field("group_id", Type.COMPACT_STRING, "The group ID string."),
                new Field("group_state", Type.COMPACT_STRING, "The group state string, or the empty string."),
                new Field("group_epoch", Type.INT32, "The group epoch."),
                new Field("assignment_epoch", Type.INT32, "The assignment epoch."),
                new Field("assignor_name", Type.COMPACT_STRING, "The selected assignor."),
                new Field("members", new CompactArrayOf(Member.SCHEMA_1), "The members."),
                new Field("authorized_operations", Type.INT32, "32-bit bitfield to represent authorized operations for this group."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public DescribedGroup(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public DescribedGroup() {
            this.errorCode = (short) 0;
            this.errorMessage = null;
            this.groupId = "";
            this.groupState = "";
            this.groupEpoch = 0;
            this.assignmentEpoch = 0;
            this.assignorName = "";
            this.members = new ArrayList<Member>(0);
            this.authorizedOperations = -2147483648;
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of DescribedGroup");
            }
            this.errorCode = _readable.readShort();
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    this.errorMessage = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field errorMessage had invalid length " + length);
                } else {
                    this.errorMessage = _readable.readString(length);
                }
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field groupId was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field groupId had invalid length " + length);
                } else {
                    this.groupId = _readable.readString(length);
                }
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field groupState was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field groupState had invalid length " + length);
                } else {
                    this.groupState = _readable.readString(length);
                }
            }
            this.groupEpoch = _readable.readInt();
            this.assignmentEpoch = _readable.readInt();
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field assignorName was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field assignorName had invalid length " + length);
                } else {
                    this.assignorName = _readable.readString(length);
                }
            }
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field members was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<Member> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new Member(_readable, _version));
                    }
                    this.members = newCollection;
                }
            }
            this.authorizedOperations = _readable.readInt();
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
            if (errorMessage == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(errorMessage);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            {
                byte[] _stringBytes = _cache.getSerializedValue(groupId);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            {
                byte[] _stringBytes = _cache.getSerializedValue(groupState);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeInt(groupEpoch);
            _writable.writeInt(assignmentEpoch);
            {
                byte[] _stringBytes = _cache.getSerializedValue(assignorName);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeUnsignedVarint(members.size() + 1);
            for (Member membersElement : members) {
                membersElement.write(_writable, _cache, _version);
            }
            _writable.writeInt(authorizedOperations);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of DescribedGroup");
            }
            _size.addBytes(2);
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
            {
                byte[] _stringBytes = groupId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'groupId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(groupId, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                byte[] _stringBytes = groupState.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'groupState' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(groupState, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            _size.addBytes(4);
            _size.addBytes(4);
            {
                byte[] _stringBytes = assignorName.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'assignorName' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(assignorName, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(members.size() + 1));
                for (Member membersElement : members) {
                    membersElement.addSize(_size, _cache, _version);
                }
            }
            _size.addBytes(4);
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
            if (!(obj instanceof DescribedGroup)) return false;
            DescribedGroup other = (DescribedGroup) obj;
            if (errorCode != other.errorCode) return false;
            if (this.errorMessage == null) {
                if (other.errorMessage != null) return false;
            } else {
                if (!this.errorMessage.equals(other.errorMessage)) return false;
            }
            if (this.groupId == null) {
                if (other.groupId != null) return false;
            } else {
                if (!this.groupId.equals(other.groupId)) return false;
            }
            if (this.groupState == null) {
                if (other.groupState != null) return false;
            } else {
                if (!this.groupState.equals(other.groupState)) return false;
            }
            if (groupEpoch != other.groupEpoch) return false;
            if (assignmentEpoch != other.assignmentEpoch) return false;
            if (this.assignorName == null) {
                if (other.assignorName != null) return false;
            } else {
                if (!this.assignorName.equals(other.assignorName)) return false;
            }
            if (this.members == null) {
                if (other.members != null) return false;
            } else {
                if (!this.members.equals(other.members)) return false;
            }
            if (authorizedOperations != other.authorizedOperations) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + errorCode;
            hashCode = 31 * hashCode + (errorMessage == null ? 0 : errorMessage.hashCode());
            hashCode = 31 * hashCode + (groupId == null ? 0 : groupId.hashCode());
            hashCode = 31 * hashCode + (groupState == null ? 0 : groupState.hashCode());
            hashCode = 31 * hashCode + groupEpoch;
            hashCode = 31 * hashCode + assignmentEpoch;
            hashCode = 31 * hashCode + (assignorName == null ? 0 : assignorName.hashCode());
            hashCode = 31 * hashCode + (members == null ? 0 : members.hashCode());
            hashCode = 31 * hashCode + authorizedOperations;
            return hashCode;
        }
        
        @Override
        public DescribedGroup duplicate() {
            DescribedGroup _duplicate = new DescribedGroup();
            _duplicate.errorCode = errorCode;
            if (errorMessage == null) {
                _duplicate.errorMessage = null;
            } else {
                _duplicate.errorMessage = errorMessage;
            }
            _duplicate.groupId = groupId;
            _duplicate.groupState = groupState;
            _duplicate.groupEpoch = groupEpoch;
            _duplicate.assignmentEpoch = assignmentEpoch;
            _duplicate.assignorName = assignorName;
            ArrayList<Member> newMembers = new ArrayList<Member>(members.size());
            for (Member _element : members) {
                newMembers.add(_element.duplicate());
            }
            _duplicate.members = newMembers;
            _duplicate.authorizedOperations = authorizedOperations;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "DescribedGroup("
                + "errorCode=" + errorCode
                + ", errorMessage=" + ((errorMessage == null) ? "null" : "'" + errorMessage.toString() + "'")
                + ", groupId=" + ((groupId == null) ? "null" : "'" + groupId.toString() + "'")
                + ", groupState=" + ((groupState == null) ? "null" : "'" + groupState.toString() + "'")
                + ", groupEpoch=" + groupEpoch
                + ", assignmentEpoch=" + assignmentEpoch
                + ", assignorName=" + ((assignorName == null) ? "null" : "'" + assignorName.toString() + "'")
                + ", members=" + MessageUtil.deepToString(members.iterator())
                + ", authorizedOperations=" + authorizedOperations
                + ")";
        }
        
        public short errorCode() {
            return this.errorCode;
        }
        
        public String errorMessage() {
            return this.errorMessage;
        }
        
        public String groupId() {
            return this.groupId;
        }
        
        public String groupState() {
            return this.groupState;
        }
        
        public int groupEpoch() {
            return this.groupEpoch;
        }
        
        public int assignmentEpoch() {
            return this.assignmentEpoch;
        }
        
        public String assignorName() {
            return this.assignorName;
        }
        
        public List<Member> members() {
            return this.members;
        }
        
        public int authorizedOperations() {
            return this.authorizedOperations;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public DescribedGroup setErrorCode(short v) {
            this.errorCode = v;
            return this;
        }
        
        public DescribedGroup setErrorMessage(String v) {
            this.errorMessage = v;
            return this;
        }
        
        public DescribedGroup setGroupId(String v) {
            this.groupId = v;
            return this;
        }
        
        public DescribedGroup setGroupState(String v) {
            this.groupState = v;
            return this;
        }
        
        public DescribedGroup setGroupEpoch(int v) {
            this.groupEpoch = v;
            return this;
        }
        
        public DescribedGroup setAssignmentEpoch(int v) {
            this.assignmentEpoch = v;
            return this;
        }
        
        public DescribedGroup setAssignorName(String v) {
            this.assignorName = v;
            return this;
        }
        
        public DescribedGroup setMembers(List<Member> v) {
            this.members = v;
            return this;
        }
        
        public DescribedGroup setAuthorizedOperations(int v) {
            this.authorizedOperations = v;
            return this;
        }
    }
    
    public static class Member implements Message {
        String memberId;
        String instanceId;
        String rackId;
        int memberEpoch;
        String clientId;
        String clientHost;
        List<String> subscribedTopicNames;
        String subscribedTopicRegex;
        Assignment assignment;
        Assignment targetAssignment;
        byte memberType;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("member_id", Type.COMPACT_STRING, "The member ID."),
                new Field("instance_id", Type.COMPACT_NULLABLE_STRING, "The member instance ID."),
                new Field("rack_id", Type.COMPACT_NULLABLE_STRING, "The member rack ID."),
                new Field("member_epoch", Type.INT32, "The current member epoch."),
                new Field("client_id", Type.COMPACT_STRING, "The client ID."),
                new Field("client_host", Type.COMPACT_STRING, "The client host."),
                new Field("subscribed_topic_names", new CompactArrayOf(Type.COMPACT_STRING), "The subscribed topic names."),
                new Field("subscribed_topic_regex", Type.COMPACT_NULLABLE_STRING, "the subscribed topic regex otherwise or null of not provided."),
                new Field("assignment", Assignment.SCHEMA_0, "The current assignment."),
                new Field("target_assignment", Assignment.SCHEMA_0, "The target assignment."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("member_id", Type.COMPACT_STRING, "The member ID."),
                new Field("instance_id", Type.COMPACT_NULLABLE_STRING, "The member instance ID."),
                new Field("rack_id", Type.COMPACT_NULLABLE_STRING, "The member rack ID."),
                new Field("member_epoch", Type.INT32, "The current member epoch."),
                new Field("client_id", Type.COMPACT_STRING, "The client ID."),
                new Field("client_host", Type.COMPACT_STRING, "The client host."),
                new Field("subscribed_topic_names", new CompactArrayOf(Type.COMPACT_STRING), "The subscribed topic names."),
                new Field("subscribed_topic_regex", Type.COMPACT_NULLABLE_STRING, "the subscribed topic regex otherwise or null of not provided."),
                new Field("assignment", Assignment.SCHEMA_0, "The current assignment."),
                new Field("target_assignment", Assignment.SCHEMA_0, "The target assignment."),
                new Field("member_type", Type.INT8, "-1 for unknown. 0 for classic member. +1 for consumer member."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public Member(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public Member() {
            this.memberId = "";
            this.instanceId = null;
            this.rackId = null;
            this.memberEpoch = 0;
            this.clientId = "";
            this.clientHost = "";
            this.subscribedTopicNames = new ArrayList<String>(0);
            this.subscribedTopicRegex = null;
            this.assignment = new Assignment();
            this.targetAssignment = new Assignment();
            this.memberType = (byte) -1;
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of Member");
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field memberId was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field memberId had invalid length " + length);
                } else {
                    this.memberId = _readable.readString(length);
                }
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    this.instanceId = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field instanceId had invalid length " + length);
                } else {
                    this.instanceId = _readable.readString(length);
                }
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    this.rackId = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field rackId had invalid length " + length);
                } else {
                    this.rackId = _readable.readString(length);
                }
            }
            this.memberEpoch = _readable.readInt();
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field clientId was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field clientId had invalid length " + length);
                } else {
                    this.clientId = _readable.readString(length);
                }
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field clientHost was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field clientHost had invalid length " + length);
                } else {
                    this.clientHost = _readable.readString(length);
                }
            }
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field subscribedTopicNames was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<String> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        int length;
                        length = _readable.readUnsignedVarint() - 1;
                        if (length < 0) {
                            throw new RuntimeException("non-nullable field subscribedTopicNames element was serialized as null");
                        } else if (length > 0x7fff) {
                            throw new RuntimeException("string field subscribedTopicNames element had invalid length " + length);
                        } else {
                            newCollection.add(_readable.readString(length));
                        }
                    }
                    this.subscribedTopicNames = newCollection;
                }
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    this.subscribedTopicRegex = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field subscribedTopicRegex had invalid length " + length);
                } else {
                    this.subscribedTopicRegex = _readable.readString(length);
                }
            }
            {
                this.assignment = new Assignment(_readable, _version);
            }
            {
                this.targetAssignment = new Assignment(_readable, _version);
            }
            if (_version >= 1) {
                this.memberType = _readable.readByte();
            } else {
                this.memberType = (byte) -1;
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
                byte[] _stringBytes = _cache.getSerializedValue(memberId);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            if (instanceId == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(instanceId);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            if (rackId == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(rackId);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeInt(memberEpoch);
            {
                byte[] _stringBytes = _cache.getSerializedValue(clientId);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            {
                byte[] _stringBytes = _cache.getSerializedValue(clientHost);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeUnsignedVarint(subscribedTopicNames.size() + 1);
            for (String subscribedTopicNamesElement : subscribedTopicNames) {
                {
                    byte[] _stringBytes = _cache.getSerializedValue(subscribedTopicNamesElement);
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                    _writable.writeByteArray(_stringBytes);
                }
            }
            if (subscribedTopicRegex == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(subscribedTopicRegex);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            assignment.write(_writable, _cache, _version);
            targetAssignment.write(_writable, _cache, _version);
            if (_version >= 1) {
                _writable.writeByte(memberType);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 1) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of Member");
            }
            {
                byte[] _stringBytes = memberId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'memberId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(memberId, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            if (instanceId == null) {
                _size.addBytes(1);
            } else {
                byte[] _stringBytes = instanceId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'instanceId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(instanceId, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            if (rackId == null) {
                _size.addBytes(1);
            } else {
                byte[] _stringBytes = rackId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'rackId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(rackId, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            _size.addBytes(4);
            {
                byte[] _stringBytes = clientId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'clientId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(clientId, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                byte[] _stringBytes = clientHost.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'clientHost' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(clientHost, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(subscribedTopicNames.size() + 1));
                for (String subscribedTopicNamesElement : subscribedTopicNames) {
                    byte[] _stringBytes = subscribedTopicNamesElement.getBytes(StandardCharsets.UTF_8);
                    if (_stringBytes.length > 0x7fff) {
                        throw new RuntimeException("'subscribedTopicNamesElement' field is too long to be serialized");
                    }
                    _cache.cacheSerializedValue(subscribedTopicNamesElement, _stringBytes);
                    _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                }
            }
            if (subscribedTopicRegex == null) {
                _size.addBytes(1);
            } else {
                byte[] _stringBytes = subscribedTopicRegex.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'subscribedTopicRegex' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(subscribedTopicRegex, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                this.assignment.addSize(_size, _cache, _version);
            }
            {
                this.targetAssignment.addSize(_size, _cache, _version);
            }
            if (_version >= 1) {
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
            if (!(obj instanceof Member)) return false;
            Member other = (Member) obj;
            if (this.memberId == null) {
                if (other.memberId != null) return false;
            } else {
                if (!this.memberId.equals(other.memberId)) return false;
            }
            if (this.instanceId == null) {
                if (other.instanceId != null) return false;
            } else {
                if (!this.instanceId.equals(other.instanceId)) return false;
            }
            if (this.rackId == null) {
                if (other.rackId != null) return false;
            } else {
                if (!this.rackId.equals(other.rackId)) return false;
            }
            if (memberEpoch != other.memberEpoch) return false;
            if (this.clientId == null) {
                if (other.clientId != null) return false;
            } else {
                if (!this.clientId.equals(other.clientId)) return false;
            }
            if (this.clientHost == null) {
                if (other.clientHost != null) return false;
            } else {
                if (!this.clientHost.equals(other.clientHost)) return false;
            }
            if (this.subscribedTopicNames == null) {
                if (other.subscribedTopicNames != null) return false;
            } else {
                if (!this.subscribedTopicNames.equals(other.subscribedTopicNames)) return false;
            }
            if (this.subscribedTopicRegex == null) {
                if (other.subscribedTopicRegex != null) return false;
            } else {
                if (!this.subscribedTopicRegex.equals(other.subscribedTopicRegex)) return false;
            }
            if (this.assignment == null) {
                if (other.assignment != null) return false;
            } else {
                if (!this.assignment.equals(other.assignment)) return false;
            }
            if (this.targetAssignment == null) {
                if (other.targetAssignment != null) return false;
            } else {
                if (!this.targetAssignment.equals(other.targetAssignment)) return false;
            }
            if (memberType != other.memberType) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (memberId == null ? 0 : memberId.hashCode());
            hashCode = 31 * hashCode + (instanceId == null ? 0 : instanceId.hashCode());
            hashCode = 31 * hashCode + (rackId == null ? 0 : rackId.hashCode());
            hashCode = 31 * hashCode + memberEpoch;
            hashCode = 31 * hashCode + (clientId == null ? 0 : clientId.hashCode());
            hashCode = 31 * hashCode + (clientHost == null ? 0 : clientHost.hashCode());
            hashCode = 31 * hashCode + (subscribedTopicNames == null ? 0 : subscribedTopicNames.hashCode());
            hashCode = 31 * hashCode + (subscribedTopicRegex == null ? 0 : subscribedTopicRegex.hashCode());
            hashCode = 31 * hashCode + (assignment == null ? 0 : assignment.hashCode());
            hashCode = 31 * hashCode + (targetAssignment == null ? 0 : targetAssignment.hashCode());
            hashCode = 31 * hashCode + memberType;
            return hashCode;
        }
        
        @Override
        public Member duplicate() {
            Member _duplicate = new Member();
            _duplicate.memberId = memberId;
            if (instanceId == null) {
                _duplicate.instanceId = null;
            } else {
                _duplicate.instanceId = instanceId;
            }
            if (rackId == null) {
                _duplicate.rackId = null;
            } else {
                _duplicate.rackId = rackId;
            }
            _duplicate.memberEpoch = memberEpoch;
            _duplicate.clientId = clientId;
            _duplicate.clientHost = clientHost;
            ArrayList<String> newSubscribedTopicNames = new ArrayList<String>(subscribedTopicNames.size());
            for (String _element : subscribedTopicNames) {
                newSubscribedTopicNames.add(_element);
            }
            _duplicate.subscribedTopicNames = newSubscribedTopicNames;
            if (subscribedTopicRegex == null) {
                _duplicate.subscribedTopicRegex = null;
            } else {
                _duplicate.subscribedTopicRegex = subscribedTopicRegex;
            }
            _duplicate.assignment = assignment.duplicate();
            _duplicate.targetAssignment = targetAssignment.duplicate();
            _duplicate.memberType = memberType;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Member("
                + "memberId=" + ((memberId == null) ? "null" : "'" + memberId.toString() + "'")
                + ", instanceId=" + ((instanceId == null) ? "null" : "'" + instanceId.toString() + "'")
                + ", rackId=" + ((rackId == null) ? "null" : "'" + rackId.toString() + "'")
                + ", memberEpoch=" + memberEpoch
                + ", clientId=" + ((clientId == null) ? "null" : "'" + clientId.toString() + "'")
                + ", clientHost=" + ((clientHost == null) ? "null" : "'" + clientHost.toString() + "'")
                + ", subscribedTopicNames=" + MessageUtil.deepToString(subscribedTopicNames.iterator())
                + ", subscribedTopicRegex=" + ((subscribedTopicRegex == null) ? "null" : "'" + subscribedTopicRegex.toString() + "'")
                + ", assignment=" + assignment.toString()
                + ", targetAssignment=" + targetAssignment.toString()
                + ", memberType=" + memberType
                + ")";
        }
        
        public String memberId() {
            return this.memberId;
        }
        
        public String instanceId() {
            return this.instanceId;
        }
        
        public String rackId() {
            return this.rackId;
        }
        
        public int memberEpoch() {
            return this.memberEpoch;
        }
        
        public String clientId() {
            return this.clientId;
        }
        
        public String clientHost() {
            return this.clientHost;
        }
        
        public List<String> subscribedTopicNames() {
            return this.subscribedTopicNames;
        }
        
        public String subscribedTopicRegex() {
            return this.subscribedTopicRegex;
        }
        
        public Assignment assignment() {
            return this.assignment;
        }
        
        public Assignment targetAssignment() {
            return this.targetAssignment;
        }
        
        public byte memberType() {
            return this.memberType;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public Member setMemberId(String v) {
            this.memberId = v;
            return this;
        }
        
        public Member setInstanceId(String v) {
            this.instanceId = v;
            return this;
        }
        
        public Member setRackId(String v) {
            this.rackId = v;
            return this;
        }
        
        public Member setMemberEpoch(int v) {
            this.memberEpoch = v;
            return this;
        }
        
        public Member setClientId(String v) {
            this.clientId = v;
            return this;
        }
        
        public Member setClientHost(String v) {
            this.clientHost = v;
            return this;
        }
        
        public Member setSubscribedTopicNames(List<String> v) {
            this.subscribedTopicNames = v;
            return this;
        }
        
        public Member setSubscribedTopicRegex(String v) {
            this.subscribedTopicRegex = v;
            return this;
        }
        
        public Member setAssignment(Assignment v) {
            this.assignment = v;
            return this;
        }
        
        public Member setTargetAssignment(Assignment v) {
            this.targetAssignment = v;
            return this;
        }
        
        public Member setMemberType(byte v) {
            this.memberType = v;
            return this;
        }
    }
    
    public static class Assignment implements Message {
        List<TopicPartitions> topicPartitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic_partitions", new CompactArrayOf(TopicPartitions.SCHEMA_0), "The assigned topic-partitions to the member."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public Assignment(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public Assignment() {
            this.topicPartitions = new ArrayList<TopicPartitions>(0);
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
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field topicPartitions was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<TopicPartitions> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new TopicPartitions(_readable, _version));
                    }
                    this.topicPartitions = newCollection;
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
            _writable.writeUnsignedVarint(topicPartitions.size() + 1);
            for (TopicPartitions topicPartitionsElement : topicPartitions) {
                topicPartitionsElement.write(_writable, _cache, _version);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(topicPartitions.size() + 1));
                for (TopicPartitions topicPartitionsElement : topicPartitions) {
                    topicPartitionsElement.addSize(_size, _cache, _version);
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
            if (!(obj instanceof Assignment)) return false;
            Assignment other = (Assignment) obj;
            if (this.topicPartitions == null) {
                if (other.topicPartitions != null) return false;
            } else {
                if (!this.topicPartitions.equals(other.topicPartitions)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topicPartitions == null ? 0 : topicPartitions.hashCode());
            return hashCode;
        }
        
        @Override
        public Assignment duplicate() {
            Assignment _duplicate = new Assignment();
            ArrayList<TopicPartitions> newTopicPartitions = new ArrayList<TopicPartitions>(topicPartitions.size());
            for (TopicPartitions _element : topicPartitions) {
                newTopicPartitions.add(_element.duplicate());
            }
            _duplicate.topicPartitions = newTopicPartitions;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Assignment("
                + "topicPartitions=" + MessageUtil.deepToString(topicPartitions.iterator())
                + ")";
        }
        
        public List<TopicPartitions> topicPartitions() {
            return this.topicPartitions;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public Assignment setTopicPartitions(List<TopicPartitions> v) {
            this.topicPartitions = v;
            return this;
        }
    }
    
    public static class TopicPartitions implements Message {
        Uuid topicId;
        String topicName;
        List<Integer> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic_id", Type.UUID, "The topic ID."),
                new Field("topic_name", Type.COMPACT_STRING, "The topic name."),
                new Field("partitions", new CompactArrayOf(Type.INT32), "The partitions."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;
        
        public TopicPartitions(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TopicPartitions() {
            this.topicId = Uuid.ZERO_UUID;
            this.topicName = "";
            this.partitions = new ArrayList<Integer>(0);
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
            this.topicId = _readable.readUuid();
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
                    ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(_readable.readInt());
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
            _writable.writeUuid(topicId);
            {
                byte[] _stringBytes = _cache.getSerializedValue(topicName);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeUnsignedVarint(partitions.size() + 1);
            for (Integer partitionsElement : partitions) {
                _writable.writeInt(partitionsElement);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _size.addBytes(16);
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
                _size.addBytes(partitions.size() * 4);
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
            if (!(obj instanceof TopicPartitions)) return false;
            TopicPartitions other = (TopicPartitions) obj;
            if (!this.topicId.equals(other.topicId)) return false;
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
            hashCode = 31 * hashCode + topicId.hashCode();
            hashCode = 31 * hashCode + (topicName == null ? 0 : topicName.hashCode());
            hashCode = 31 * hashCode + (partitions == null ? 0 : partitions.hashCode());
            return hashCode;
        }
        
        @Override
        public TopicPartitions duplicate() {
            TopicPartitions _duplicate = new TopicPartitions();
            _duplicate.topicId = topicId;
            _duplicate.topicName = topicName;
            ArrayList<Integer> newPartitions = new ArrayList<Integer>(partitions.size());
            for (Integer _element : partitions) {
                newPartitions.add(_element);
            }
            _duplicate.partitions = newPartitions;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TopicPartitions("
                + "topicId=" + topicId.toString()
                + ", topicName=" + ((topicName == null) ? "null" : "'" + topicName.toString() + "'")
                + ", partitions=" + MessageUtil.deepToString(partitions.iterator())
                + ")";
        }
        
        public Uuid topicId() {
            return this.topicId;
        }
        
        public String topicName() {
            return this.topicName;
        }
        
        public List<Integer> partitions() {
            return this.partitions;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TopicPartitions setTopicId(Uuid v) {
            this.topicId = v;
            return this;
        }
        
        public TopicPartitions setTopicName(String v) {
            this.topicName = v;
            return this;
        }
        
        public TopicPartitions setPartitions(List<Integer> v) {
            this.partitions = v;
            return this;
        }
    }
}
