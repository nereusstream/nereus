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
import org.apache.kafka.common.protocol.ApiMessage;
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


public class ConsumerGroupDescribeRequestData implements ApiMessage {
    List<String> groupIds;
    boolean includeAuthorizedOperations;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("group_ids", new CompactArrayOf(Type.COMPACT_STRING), "The ids of the groups to describe."),
            new Field("include_authorized_operations", Type.BOOLEAN, "Whether to include authorized operations."),
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
    
    public ConsumerGroupDescribeRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public ConsumerGroupDescribeRequestData() {
        this.groupIds = new ArrayList<String>(0);
        this.includeAuthorizedOperations = false;
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
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field groupIds was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ArrayList<String> newCollection = new ArrayList<>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    int length;
                    length = _readable.readUnsignedVarint() - 1;
                    if (length < 0) {
                        throw new RuntimeException("non-nullable field groupIds element was serialized as null");
                    } else if (length > 0x7fff) {
                        throw new RuntimeException("string field groupIds element had invalid length " + length);
                    } else {
                        newCollection.add(_readable.readString(length));
                    }
                }
                this.groupIds = newCollection;
            }
        }
        this.includeAuthorizedOperations = _readable.readByte() != 0;
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
        _writable.writeUnsignedVarint(groupIds.size() + 1);
        for (String groupIdsElement : groupIds) {
            {
                byte[] _stringBytes = _cache.getSerializedValue(groupIdsElement);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
        }
        _writable.writeByte(includeAuthorizedOperations ? (byte) 1 : (byte) 0);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(groupIds.size() + 1));
            for (String groupIdsElement : groupIds) {
                byte[] _stringBytes = groupIdsElement.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'groupIdsElement' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(groupIdsElement, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
        }
        _size.addBytes(1);
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
        if (!(obj instanceof ConsumerGroupDescribeRequestData)) return false;
        ConsumerGroupDescribeRequestData other = (ConsumerGroupDescribeRequestData) obj;
        if (this.groupIds == null) {
            if (other.groupIds != null) return false;
        } else {
            if (!this.groupIds.equals(other.groupIds)) return false;
        }
        if (includeAuthorizedOperations != other.includeAuthorizedOperations) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (groupIds == null ? 0 : groupIds.hashCode());
        hashCode = 31 * hashCode + (includeAuthorizedOperations ? 1231 : 1237);
        return hashCode;
    }
    
    @Override
    public ConsumerGroupDescribeRequestData duplicate() {
        ConsumerGroupDescribeRequestData _duplicate = new ConsumerGroupDescribeRequestData();
        ArrayList<String> newGroupIds = new ArrayList<String>(groupIds.size());
        for (String _element : groupIds) {
            newGroupIds.add(_element);
        }
        _duplicate.groupIds = newGroupIds;
        _duplicate.includeAuthorizedOperations = includeAuthorizedOperations;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "ConsumerGroupDescribeRequestData("
            + "groupIds=" + MessageUtil.deepToString(groupIds.iterator())
            + ", includeAuthorizedOperations=" + (includeAuthorizedOperations ? "true" : "false")
            + ")";
    }
    
    public List<String> groupIds() {
        return this.groupIds;
    }
    
    public boolean includeAuthorizedOperations() {
        return this.includeAuthorizedOperations;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public ConsumerGroupDescribeRequestData setGroupIds(List<String> v) {
        this.groupIds = v;
        return this;
    }
    
    public ConsumerGroupDescribeRequestData setIncludeAuthorizedOperations(boolean v) {
        this.includeAuthorizedOperations = v;
        return this;
    }
}
