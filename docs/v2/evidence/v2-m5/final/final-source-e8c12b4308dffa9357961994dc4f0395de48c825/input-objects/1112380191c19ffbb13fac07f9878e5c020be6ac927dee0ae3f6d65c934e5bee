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


public class UpdateRaftVoterRequestData implements ApiMessage {
    String clusterId;
    int currentLeaderEpoch;
    int voterId;
    Uuid voterDirectoryId;
    ListenerCollection listeners;
    KRaftVersionFeature kRaftVersionFeature;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("cluster_id", Type.COMPACT_NULLABLE_STRING, "The cluster id."),
            new Field("current_leader_epoch", Type.INT32, "The current leader epoch of the partition, -1 for unknown leader epoch."),
            new Field("voter_id", Type.INT32, "The replica id of the voter getting updated in the topic partition."),
            new Field("voter_directory_id", Type.UUID, "The directory id of the voter getting updated in the topic partition."),
            new Field("listeners", new CompactArrayOf(Listener.SCHEMA_0), "The endpoint that can be used to communicate with the leader."),
            new Field("kraft_version_feature", KRaftVersionFeature.SCHEMA_0, "The range of versions of the protocol that the replica supports."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public UpdateRaftVoterRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public UpdateRaftVoterRequestData() {
        this.clusterId = "";
        this.currentLeaderEpoch = 0;
        this.voterId = 0;
        this.voterDirectoryId = Uuid.ZERO_UUID;
        this.listeners = new ListenerCollection(0);
        this.kRaftVersionFeature = new KRaftVersionFeature();
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
        this.currentLeaderEpoch = _readable.readInt();
        this.voterId = _readable.readInt();
        this.voterDirectoryId = _readable.readUuid();
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
        if (clusterId == null) {
            _writable.writeUnsignedVarint(0);
        } else {
            byte[] _stringBytes = _cache.getSerializedValue(clusterId);
            _writable.writeUnsignedVarint(_stringBytes.length + 1);
            _writable.writeByteArray(_stringBytes);
        }
        _writable.writeInt(currentLeaderEpoch);
        _writable.writeInt(voterId);
        _writable.writeUuid(voterDirectoryId);
        _writable.writeUnsignedVarint(listeners.size() + 1);
        for (Listener listenersElement : listeners) {
            listenersElement.write(_writable, _cache, _version);
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
        _size.addBytes(4);
        _size.addBytes(16);
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(listeners.size() + 1));
            for (Listener listenersElement : listeners) {
                listenersElement.addSize(_size, _cache, _version);
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
        if (!(obj instanceof UpdateRaftVoterRequestData)) return false;
        UpdateRaftVoterRequestData other = (UpdateRaftVoterRequestData) obj;
        if (this.clusterId == null) {
            if (other.clusterId != null) return false;
        } else {
            if (!this.clusterId.equals(other.clusterId)) return false;
        }
        if (currentLeaderEpoch != other.currentLeaderEpoch) return false;
        if (voterId != other.voterId) return false;
        if (!this.voterDirectoryId.equals(other.voterDirectoryId)) return false;
        if (this.listeners == null) {
            if (other.listeners != null) return false;
        } else {
            if (!this.listeners.equals(other.listeners)) return false;
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
        hashCode = 31 * hashCode + (clusterId == null ? 0 : clusterId.hashCode());
        hashCode = 31 * hashCode + currentLeaderEpoch;
        hashCode = 31 * hashCode + voterId;
        hashCode = 31 * hashCode + voterDirectoryId.hashCode();
        hashCode = 31 * hashCode + (listeners == null ? 0 : listeners.hashCode());
        hashCode = 31 * hashCode + (kRaftVersionFeature == null ? 0 : kRaftVersionFeature.hashCode());
        return hashCode;
    }
    
    @Override
    public UpdateRaftVoterRequestData duplicate() {
        UpdateRaftVoterRequestData _duplicate = new UpdateRaftVoterRequestData();
        if (clusterId == null) {
            _duplicate.clusterId = null;
        } else {
            _duplicate.clusterId = clusterId;
        }
        _duplicate.currentLeaderEpoch = currentLeaderEpoch;
        _duplicate.voterId = voterId;
        _duplicate.voterDirectoryId = voterDirectoryId;
        ListenerCollection newListeners = new ListenerCollection(listeners.size());
        for (Listener _element : listeners) {
            newListeners.add(_element.duplicate());
        }
        _duplicate.listeners = newListeners;
        _duplicate.kRaftVersionFeature = kRaftVersionFeature.duplicate();
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "UpdateRaftVoterRequestData("
            + "clusterId=" + ((clusterId == null) ? "null" : "'" + clusterId.toString() + "'")
            + ", currentLeaderEpoch=" + currentLeaderEpoch
            + ", voterId=" + voterId
            + ", voterDirectoryId=" + voterDirectoryId.toString()
            + ", listeners=" + MessageUtil.deepToString(listeners.iterator())
            + ", kRaftVersionFeature=" + kRaftVersionFeature.toString()
            + ")";
    }
    
    public String clusterId() {
        return this.clusterId;
    }
    
    public int currentLeaderEpoch() {
        return this.currentLeaderEpoch;
    }
    
    public int voterId() {
        return this.voterId;
    }
    
    public Uuid voterDirectoryId() {
        return this.voterDirectoryId;
    }
    
    public ListenerCollection listeners() {
        return this.listeners;
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
    
    public UpdateRaftVoterRequestData setClusterId(String v) {
        this.clusterId = v;
        return this;
    }
    
    public UpdateRaftVoterRequestData setCurrentLeaderEpoch(int v) {
        this.currentLeaderEpoch = v;
        return this;
    }
    
    public UpdateRaftVoterRequestData setVoterId(int v) {
        this.voterId = v;
        return this;
    }
    
    public UpdateRaftVoterRequestData setVoterDirectoryId(Uuid v) {
        this.voterDirectoryId = v;
        return this;
    }
    
    public UpdateRaftVoterRequestData setListeners(ListenerCollection v) {
        this.listeners = v;
        return this;
    }
    
    public UpdateRaftVoterRequestData setKRaftVersionFeature(KRaftVersionFeature v) {
        this.kRaftVersionFeature = v;
        return this;
    }
    
    public static class Listener implements Message, ImplicitLinkedHashMultiCollection.Element {
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
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 0) {
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
            if (_version > 0) {
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
