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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
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


public class UpdateRaftVoterResponseData implements ApiMessage {
    int throttleTimeMs;
    short errorCode;
    CurrentLeader currentLeader;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The error code, or 0 if there was no error."),
            TaggedFieldsSection.of(
                0, new Field("current_leader", CurrentLeader.SCHEMA_0, "Details of the current Raft cluster leader.")
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public UpdateRaftVoterResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public UpdateRaftVoterResponseData() {
        this.throttleTimeMs = 0;
        this.errorCode = (short) 0;
        this.currentLeader = new CurrentLeader();
    }
    
    @Override
    public short apiKey() {
        return 82;
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
        this.throttleTimeMs = _readable.readInt();
        this.errorCode = _readable.readShort();
        {
            this.currentLeader = new CurrentLeader();
        }
        this._unknownTaggedFields = null;
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; _i++) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                case 0: {
                    this.currentLeader = new CurrentLeader(_readable, _version);
                    break;
                }
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
        _writable.writeShort(errorCode);
        if (!this.currentLeader.equals(new CurrentLeader())) {
            _numTaggedFields++;
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        {
            if (!this.currentLeader.equals(new CurrentLeader())) {
                _writable.writeUnsignedVarint(0);
                _writable.writeUnsignedVarint(this.currentLeader.size(_cache, _version));
                currentLeader.write(_writable, _cache, _version);
            }
        }
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(4);
        _size.addBytes(2);
        {
            if (!this.currentLeader.equals(new CurrentLeader())) {
                _numTaggedFields++;
                _size.addBytes(1);
                int _sizeBeforeStruct = _size.totalSize();
                this.currentLeader.addSize(_size, _cache, _version);
                int _structSize = _size.totalSize() - _sizeBeforeStruct;
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_structSize));
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
        if (!(obj instanceof UpdateRaftVoterResponseData)) return false;
        UpdateRaftVoterResponseData other = (UpdateRaftVoterResponseData) obj;
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (errorCode != other.errorCode) return false;
        if (this.currentLeader == null) {
            if (other.currentLeader != null) return false;
        } else {
            if (!this.currentLeader.equals(other.currentLeader)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + (currentLeader == null ? 0 : currentLeader.hashCode());
        return hashCode;
    }
    
    @Override
    public UpdateRaftVoterResponseData duplicate() {
        UpdateRaftVoterResponseData _duplicate = new UpdateRaftVoterResponseData();
        _duplicate.throttleTimeMs = throttleTimeMs;
        _duplicate.errorCode = errorCode;
        _duplicate.currentLeader = currentLeader.duplicate();
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "UpdateRaftVoterResponseData("
            + "throttleTimeMs=" + throttleTimeMs
            + ", errorCode=" + errorCode
            + ", currentLeader=" + currentLeader.toString()
            + ")";
    }
    
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public CurrentLeader currentLeader() {
        return this.currentLeader;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public UpdateRaftVoterResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }
    
    public UpdateRaftVoterResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public UpdateRaftVoterResponseData setCurrentLeader(CurrentLeader v) {
        this.currentLeader = v;
        return this;
    }
    
    public static class CurrentLeader implements Message {
        int leaderId;
        int leaderEpoch;
        String host;
        int port;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("leader_id", Type.INT32, "The replica id of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch."),
                new Field("host", Type.COMPACT_STRING, "The node's hostname."),
                new Field("port", Type.INT32, "The node's port."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public CurrentLeader(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public CurrentLeader() {
            this.leaderId = -1;
            this.leaderEpoch = -1;
            this.host = "";
            this.port = 0;
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
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of CurrentLeader");
            }
            this.leaderId = _readable.readInt();
            this.leaderEpoch = _readable.readInt();
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
            this.port = _readable.readInt();
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
            _writable.writeInt(leaderId);
            _writable.writeInt(leaderEpoch);
            {
                byte[] _stringBytes = _cache.getSerializedValue(host);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeInt(port);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of CurrentLeader");
            }
            _size.addBytes(4);
            _size.addBytes(4);
            {
                byte[] _stringBytes = host.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'host' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(host, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
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
            if (!(obj instanceof CurrentLeader)) return false;
            CurrentLeader other = (CurrentLeader) obj;
            if (leaderId != other.leaderId) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
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
            hashCode = 31 * hashCode + leaderId;
            hashCode = 31 * hashCode + leaderEpoch;
            hashCode = 31 * hashCode + (host == null ? 0 : host.hashCode());
            hashCode = 31 * hashCode + port;
            return hashCode;
        }
        
        @Override
        public CurrentLeader duplicate() {
            CurrentLeader _duplicate = new CurrentLeader();
            _duplicate.leaderId = leaderId;
            _duplicate.leaderEpoch = leaderEpoch;
            _duplicate.host = host;
            _duplicate.port = port;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "CurrentLeader("
                + "leaderId=" + leaderId
                + ", leaderEpoch=" + leaderEpoch
                + ", host=" + ((host == null) ? "null" : "'" + host.toString() + "'")
                + ", port=" + port
                + ")";
        }
        
        public int leaderId() {
            return this.leaderId;
        }
        
        public int leaderEpoch() {
            return this.leaderEpoch;
        }
        
        public String host() {
            return this.host;
        }
        
        public int port() {
            return this.port;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public CurrentLeader setLeaderId(int v) {
            this.leaderId = v;
            return this;
        }
        
        public CurrentLeader setLeaderEpoch(int v) {
            this.leaderEpoch = v;
            return this;
        }
        
        public CurrentLeader setHost(String v) {
            this.host = v;
            return this;
        }
        
        public CurrentLeader setPort(int v) {
            this.port = v;
            return this;
        }
    }
}
