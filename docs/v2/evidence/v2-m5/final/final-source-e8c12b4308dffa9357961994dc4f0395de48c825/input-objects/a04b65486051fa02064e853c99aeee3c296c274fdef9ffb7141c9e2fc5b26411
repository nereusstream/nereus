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
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class RemoveRaftVoterRequestData implements ApiMessage {
    String clusterId;
    int voterId;
    Uuid voterDirectoryId;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("cluster_id", Type.COMPACT_NULLABLE_STRING, "The cluster id of the request."),
            new Field("voter_id", Type.INT32, "The replica id of the voter getting removed from the topic partition."),
            new Field("voter_directory_id", Type.UUID, "The directory id of the voter getting removed from the topic partition."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public RemoveRaftVoterRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public RemoveRaftVoterRequestData() {
        this.clusterId = "";
        this.voterId = 0;
        this.voterDirectoryId = Uuid.ZERO_UUID;
    }
    
    @Override
    public short apiKey() {
        return 81;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 0;
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
        this.voterId = _readable.readInt();
        this.voterDirectoryId = _readable.readUuid();
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
        _writable.writeInt(voterId);
        _writable.writeUuid(voterDirectoryId);
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
        if (!(obj instanceof RemoveRaftVoterRequestData)) return false;
        RemoveRaftVoterRequestData other = (RemoveRaftVoterRequestData) obj;
        if (this.clusterId == null) {
            if (other.clusterId != null) return false;
        } else {
            if (!this.clusterId.equals(other.clusterId)) return false;
        }
        if (voterId != other.voterId) return false;
        if (!this.voterDirectoryId.equals(other.voterDirectoryId)) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (clusterId == null ? 0 : clusterId.hashCode());
        hashCode = 31 * hashCode + voterId;
        hashCode = 31 * hashCode + voterDirectoryId.hashCode();
        return hashCode;
    }
    
    @Override
    public RemoveRaftVoterRequestData duplicate() {
        RemoveRaftVoterRequestData _duplicate = new RemoveRaftVoterRequestData();
        if (clusterId == null) {
            _duplicate.clusterId = null;
        } else {
            _duplicate.clusterId = clusterId;
        }
        _duplicate.voterId = voterId;
        _duplicate.voterDirectoryId = voterDirectoryId;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "RemoveRaftVoterRequestData("
            + "clusterId=" + ((clusterId == null) ? "null" : "'" + clusterId.toString() + "'")
            + ", voterId=" + voterId
            + ", voterDirectoryId=" + voterDirectoryId.toString()
            + ")";
    }
    
    public String clusterId() {
        return this.clusterId;
    }
    
    public int voterId() {
        return this.voterId;
    }
    
    public Uuid voterDirectoryId() {
        return this.voterDirectoryId;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public RemoveRaftVoterRequestData setClusterId(String v) {
        this.clusterId = v;
        return this;
    }
    
    public RemoveRaftVoterRequestData setVoterId(int v) {
        this.voterId = v;
        return this;
    }
    
    public RemoveRaftVoterRequestData setVoterDirectoryId(Uuid v) {
        this.voterDirectoryId = v;
        return this;
    }
}
