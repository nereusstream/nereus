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


public class VotersRecord implements ApiMessage {
    short version;
    List<Voter> voters;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("version", Type.INT16, "The version of the voters record."),
            new Field("voters", new CompactArrayOf(Voter.SCHEMA_0), "The set of voters in the quorum for this epoch."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public VotersRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public VotersRecord() {
        this.version = (short) 0;
        this.voters = new ArrayList<Voter>(0);
    }
    
    @Override
    public short apiKey() {
        return -1;
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
        this.version = _readable.readShort();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field voters was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ArrayList<Voter> newCollection = new ArrayList<>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new Voter(_readable, _version));
                }
                this.voters = newCollection;
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
        _writable.writeShort(version);
        _writable.writeUnsignedVarint(voters.size() + 1);
        for (Voter votersElement : voters) {
            votersElement.write(_writable, _cache, _version);
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
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(voters.size() + 1));
            for (Voter votersElement : voters) {
                votersElement.addSize(_size, _cache, _version);
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
        if (!(obj instanceof VotersRecord)) return false;
        VotersRecord other = (VotersRecord) obj;
        if (version != other.version) return false;
        if (this.voters == null) {
            if (other.voters != null) return false;
        } else {
            if (!this.voters.equals(other.voters)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + version;
        hashCode = 31 * hashCode + (voters == null ? 0 : voters.hashCode());
        return hashCode;
    }
    
    @Override
    public VotersRecord duplicate() {
        VotersRecord _duplicate = new VotersRecord();
        _duplicate.version = version;
        ArrayList<Voter> newVoters = new ArrayList<Voter>(voters.size());
        for (Voter _element : voters) {
            newVoters.add(_element.duplicate());
        }
        _duplicate.voters = newVoters;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "VotersRecord("
            + "version=" + version
            + ", voters=" + MessageUtil.deepToString(voters.iterator())
            + ")";
    }
    
    public short version() {
        return this.version;
    }
    
    public List<Voter> voters() {
        return this.voters;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public VotersRecord setVersion(short v) {
        this.version = v;
        return this;
    }
    
    public VotersRecord setVoters(List<Voter> v) {
        this.voters = v;
        return this;
    }
    
    public static class Voter implements Message {
        int voterId;
        Uuid voterDirectoryId;
        EndpointCollection endpoints;
        KRaftVersionFeature kRaftVersionFeature;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("voter_id", Type.INT32, "The replica id of the voter in the topic partition."),
                new Field("voter_directory_id", Type.UUID, "The directory id of the voter in the topic partition."),
                new Field("endpoints", new CompactArrayOf(Endpoint.SCHEMA_0), "The endpoint that can be used to communicate with the voter."),
                new Field("kraft_version_feature", KRaftVersionFeature.SCHEMA_0, "The range of versions of the protocol that the replica supports."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public Voter(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public Voter() {
            this.voterId = 0;
            this.voterDirectoryId = Uuid.ZERO_UUID;
            this.endpoints = new EndpointCollection(0);
            this.kRaftVersionFeature = new KRaftVersionFeature();
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of Voter");
            }
            this.voterId = _readable.readInt();
            this.voterDirectoryId = _readable.readUuid();
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field endpoints was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    EndpointCollection newCollection = new EndpointCollection(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new Endpoint(_readable, _version));
                    }
                    this.endpoints = newCollection;
                }
            }
            {
                this.kRaftVersionFeature = new KRaftVersionFeature(_readable, _version);
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
            _writable.writeInt(voterId);
            _writable.writeUuid(voterDirectoryId);
            _writable.writeUnsignedVarint(endpoints.size() + 1);
            for (Endpoint endpointsElement : endpoints) {
                endpointsElement.write(_writable, _cache, _version);
            }
            kRaftVersionFeature.write(_writable, _cache, _version);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of Voter");
            }
            _size.addBytes(4);
            _size.addBytes(16);
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(endpoints.size() + 1));
                for (Endpoint endpointsElement : endpoints) {
                    endpointsElement.addSize(_size, _cache, _version);
                }
            }
            {
                this.kRaftVersionFeature.addSize(_size, _cache, _version);
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
            if (!(obj instanceof Voter)) return false;
            Voter other = (Voter) obj;
            if (voterId != other.voterId) return false;
            if (!this.voterDirectoryId.equals(other.voterDirectoryId)) return false;
            if (this.endpoints == null) {
                if (other.endpoints != null) return false;
            } else {
                if (!this.endpoints.equals(other.endpoints)) return false;
            }
            if (this.kRaftVersionFeature == null) {
                if (other.kRaftVersionFeature != null) return false;
            } else {
                if (!this.kRaftVersionFeature.equals(other.kRaftVersionFeature)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + voterId;
            hashCode = 31 * hashCode + voterDirectoryId.hashCode();
            hashCode = 31 * hashCode + (endpoints == null ? 0 : endpoints.hashCode());
            hashCode = 31 * hashCode + (kRaftVersionFeature == null ? 0 : kRaftVersionFeature.hashCode());
            return hashCode;
        }
        
        @Override
        public Voter duplicate() {
            Voter _duplicate = new Voter();
            _duplicate.voterId = voterId;
            _duplicate.voterDirectoryId = voterDirectoryId;
            EndpointCollection newEndpoints = new EndpointCollection(endpoints.size());
            for (Endpoint _element : endpoints) {
                newEndpoints.add(_element.duplicate());
            }
            _duplicate.endpoints = newEndpoints;
            _duplicate.kRaftVersionFeature = kRaftVersionFeature.duplicate();
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Voter("
                + "voterId=" + voterId
                + ", voterDirectoryId=" + voterDirectoryId.toString()
                + ", endpoints=" + MessageUtil.deepToString(endpoints.iterator())
                + ", kRaftVersionFeature=" + kRaftVersionFeature.toString()
                + ")";
        }
        
        public int voterId() {
            return this.voterId;
        }
        
        public Uuid voterDirectoryId() {
            return this.voterDirectoryId;
        }
        
        public EndpointCollection endpoints() {
            return this.endpoints;
        }
        
        public KRaftVersionFeature kRaftVersionFeature() {
            return this.kRaftVersionFeature;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public Voter setVoterId(int v) {
            this.voterId = v;
            return this;
        }
        
        public Voter setVoterDirectoryId(Uuid v) {
            this.voterDirectoryId = v;
            return this;
        }
        
        public Voter setEndpoints(EndpointCollection v) {
            this.endpoints = v;
            return this;
        }
        
        public Voter setKRaftVersionFeature(KRaftVersionFeature v) {
            this.kRaftVersionFeature = v;
            return this;
        }
    }
    
    public static class Endpoint implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        String host;
        int port;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the endpoint."),
                new Field("host", Type.COMPACT_STRING, "The hostname."),
                new Field("port", Type.UINT16, "The port."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public Endpoint(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public Endpoint() {
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
            return 0;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of Endpoint");
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
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of Endpoint");
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
            if (!(obj instanceof Endpoint)) return false;
            Endpoint other = (Endpoint) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Endpoint)) return false;
            Endpoint other = (Endpoint) obj;
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
        public Endpoint duplicate() {
            Endpoint _duplicate = new Endpoint();
            _duplicate.name = name;
            _duplicate.host = host;
            _duplicate.port = port;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Endpoint("
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
        
        public Endpoint setName(String v) {
            this.name = v;
            return this;
        }
        
        public Endpoint setHost(String v) {
            this.host = v;
            return this;
        }
        
        public Endpoint setPort(int v) {
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
    
    public static class EndpointCollection extends ImplicitLinkedHashMultiCollection<Endpoint> {
        public EndpointCollection() {
            super();
        }
        
        public EndpointCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public EndpointCollection(Iterator<Endpoint> iterator) {
            super(iterator);
        }
        
        public Endpoint find(String name) {
            Endpoint _key = new Endpoint();
            _key.setName(name);
            return find(_key);
        }
        
        public List<Endpoint> findAll(String name) {
            Endpoint _key = new Endpoint();
            _key.setName(name);
            return findAll(_key);
        }
        
        public EndpointCollection duplicate() {
            EndpointCollection _duplicate = new EndpointCollection(size());
            for (Endpoint _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
    
    public static class KRaftVersionFeature implements Message {
        short minSupportedVersion;
        short maxSupportedVersion;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("min_supported_version", Type.INT16, "The minimum supported KRaft protocol version."),
                new Field("max_supported_version", Type.INT16, "The maximum supported KRaft protocol version."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public KRaftVersionFeature(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public KRaftVersionFeature() {
            this.minSupportedVersion = (short) 0;
            this.maxSupportedVersion = (short) 0;
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of KRaftVersionFeature");
            }
            this.minSupportedVersion = _readable.readShort();
            this.maxSupportedVersion = _readable.readShort();
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
            _writable.writeShort(minSupportedVersion);
            _writable.writeShort(maxSupportedVersion);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of KRaftVersionFeature");
            }
            _size.addBytes(2);
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
        public boolean equals(Object obj) {
            if (!(obj instanceof KRaftVersionFeature)) return false;
            KRaftVersionFeature other = (KRaftVersionFeature) obj;
            if (minSupportedVersion != other.minSupportedVersion) return false;
            if (maxSupportedVersion != other.maxSupportedVersion) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + minSupportedVersion;
            hashCode = 31 * hashCode + maxSupportedVersion;
            return hashCode;
        }
        
        @Override
        public KRaftVersionFeature duplicate() {
            KRaftVersionFeature _duplicate = new KRaftVersionFeature();
            _duplicate.minSupportedVersion = minSupportedVersion;
            _duplicate.maxSupportedVersion = maxSupportedVersion;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "KRaftVersionFeature("
                + "minSupportedVersion=" + minSupportedVersion
                + ", maxSupportedVersion=" + maxSupportedVersion
                + ")";
        }
        
        public short minSupportedVersion() {
            return this.minSupportedVersion;
        }
        
        public short maxSupportedVersion() {
            return this.maxSupportedVersion;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public KRaftVersionFeature setMinSupportedVersion(short v) {
            this.minSupportedVersion = v;
            return this;
        }
        
        public KRaftVersionFeature setMaxSupportedVersion(short v) {
            this.maxSupportedVersion = v;
            return this;
        }
    }
}
