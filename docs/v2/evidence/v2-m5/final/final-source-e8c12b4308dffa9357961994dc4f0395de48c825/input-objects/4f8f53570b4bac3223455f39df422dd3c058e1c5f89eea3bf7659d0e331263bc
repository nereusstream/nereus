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

package org.apache.kafka.common.metadata;

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


public class RegisterControllerRecord implements ApiMessage {
    int controllerId;
    Uuid incarnationId;
    boolean zkMigrationReady;
    ControllerEndpointCollection endPoints;
    ControllerFeatureCollection features;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("controller_id", Type.INT32, "The controller id."),
            new Field("incarnation_id", Type.UUID, "The incarnation ID of the controller process"),
            new Field("zk_migration_ready", Type.BOOLEAN, "Set if the required configurations for ZK migration are present."),
            new Field("end_points", new CompactArrayOf(ControllerEndpoint.SCHEMA_0), "The endpoints that can be used to communicate with this controller."),
            new Field("features", new CompactArrayOf(ControllerFeature.SCHEMA_0), "The features on this controller"),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public RegisterControllerRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public RegisterControllerRecord() {
        this.controllerId = 0;
        this.incarnationId = Uuid.ZERO_UUID;
        this.zkMigrationReady = false;
        this.endPoints = new ControllerEndpointCollection(0);
        this.features = new ControllerFeatureCollection(0);
    }
    
    @Override
    public short apiKey() {
        return 27;
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
        this.controllerId = _readable.readInt();
        this.incarnationId = _readable.readUuid();
        this.zkMigrationReady = _readable.readByte() != 0;
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field endPoints was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ControllerEndpointCollection newCollection = new ControllerEndpointCollection(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new ControllerEndpoint(_readable, _version));
                }
                this.endPoints = newCollection;
            }
        }
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field features was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ControllerFeatureCollection newCollection = new ControllerFeatureCollection(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new ControllerFeature(_readable, _version));
                }
                this.features = newCollection;
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
        _writable.writeInt(controllerId);
        _writable.writeUuid(incarnationId);
        _writable.writeByte(zkMigrationReady ? (byte) 1 : (byte) 0);
        _writable.writeUnsignedVarint(endPoints.size() + 1);
        for (ControllerEndpoint endPointsElement : endPoints) {
            endPointsElement.write(_writable, _cache, _version);
        }
        _writable.writeUnsignedVarint(features.size() + 1);
        for (ControllerFeature featuresElement : features) {
            featuresElement.write(_writable, _cache, _version);
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
        _size.addBytes(16);
        _size.addBytes(1);
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(endPoints.size() + 1));
            for (ControllerEndpoint endPointsElement : endPoints) {
                endPointsElement.addSize(_size, _cache, _version);
            }
        }
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(features.size() + 1));
            for (ControllerFeature featuresElement : features) {
                featuresElement.addSize(_size, _cache, _version);
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
        if (!(obj instanceof RegisterControllerRecord)) return false;
        RegisterControllerRecord other = (RegisterControllerRecord) obj;
        if (controllerId != other.controllerId) return false;
        if (!this.incarnationId.equals(other.incarnationId)) return false;
        if (zkMigrationReady != other.zkMigrationReady) return false;
        if (this.endPoints == null) {
            if (other.endPoints != null) return false;
        } else {
            if (!this.endPoints.equals(other.endPoints)) return false;
        }
        if (this.features == null) {
            if (other.features != null) return false;
        } else {
            if (!this.features.equals(other.features)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + controllerId;
        hashCode = 31 * hashCode + incarnationId.hashCode();
        hashCode = 31 * hashCode + (zkMigrationReady ? 1231 : 1237);
        hashCode = 31 * hashCode + (endPoints == null ? 0 : endPoints.hashCode());
        hashCode = 31 * hashCode + (features == null ? 0 : features.hashCode());
        return hashCode;
    }
    
    @Override
    public RegisterControllerRecord duplicate() {
        RegisterControllerRecord _duplicate = new RegisterControllerRecord();
        _duplicate.controllerId = controllerId;
        _duplicate.incarnationId = incarnationId;
        _duplicate.zkMigrationReady = zkMigrationReady;
        ControllerEndpointCollection newEndPoints = new ControllerEndpointCollection(endPoints.size());
        for (ControllerEndpoint _element : endPoints) {
            newEndPoints.add(_element.duplicate());
        }
        _duplicate.endPoints = newEndPoints;
        ControllerFeatureCollection newFeatures = new ControllerFeatureCollection(features.size());
        for (ControllerFeature _element : features) {
            newFeatures.add(_element.duplicate());
        }
        _duplicate.features = newFeatures;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "RegisterControllerRecord("
            + "controllerId=" + controllerId
            + ", incarnationId=" + incarnationId.toString()
            + ", zkMigrationReady=" + (zkMigrationReady ? "true" : "false")
            + ", endPoints=" + MessageUtil.deepToString(endPoints.iterator())
            + ", features=" + MessageUtil.deepToString(features.iterator())
            + ")";
    }
    
    public int controllerId() {
        return this.controllerId;
    }
    
    public Uuid incarnationId() {
        return this.incarnationId;
    }
    
    public boolean zkMigrationReady() {
        return this.zkMigrationReady;
    }
    
    public ControllerEndpointCollection endPoints() {
        return this.endPoints;
    }
    
    public ControllerFeatureCollection features() {
        return this.features;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public RegisterControllerRecord setControllerId(int v) {
        this.controllerId = v;
        return this;
    }
    
    public RegisterControllerRecord setIncarnationId(Uuid v) {
        this.incarnationId = v;
        return this;
    }
    
    public RegisterControllerRecord setZkMigrationReady(boolean v) {
        this.zkMigrationReady = v;
        return this;
    }
    
    public RegisterControllerRecord setEndPoints(ControllerEndpointCollection v) {
        this.endPoints = v;
        return this;
    }
    
    public RegisterControllerRecord setFeatures(ControllerFeatureCollection v) {
        this.features = v;
        return this;
    }
    
    public static class ControllerEndpoint implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        String host;
        int port;
        short securityProtocol;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the endpoint."),
                new Field("host", Type.COMPACT_STRING, "The hostname."),
                new Field("port", Type.UINT16, "The port."),
                new Field("security_protocol", Type.INT16, "The security protocol."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public ControllerEndpoint(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public ControllerEndpoint() {
            this.name = "";
            this.host = "";
            this.port = 0;
            this.securityProtocol = (short) 0;
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of ControllerEndpoint");
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
            this.securityProtocol = _readable.readShort();
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
            _writable.writeShort(securityProtocol);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of ControllerEndpoint");
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
            if (!(obj instanceof ControllerEndpoint)) return false;
            ControllerEndpoint other = (ControllerEndpoint) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ControllerEndpoint)) return false;
            ControllerEndpoint other = (ControllerEndpoint) obj;
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
            if (securityProtocol != other.securityProtocol) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            return hashCode;
        }
        
        @Override
        public ControllerEndpoint duplicate() {
            ControllerEndpoint _duplicate = new ControllerEndpoint();
            _duplicate.name = name;
            _duplicate.host = host;
            _duplicate.port = port;
            _duplicate.securityProtocol = securityProtocol;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "ControllerEndpoint("
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", host=" + ((host == null) ? "null" : "'" + host.toString() + "'")
                + ", port=" + port
                + ", securityProtocol=" + securityProtocol
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
        
        public short securityProtocol() {
            return this.securityProtocol;
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
        
        public ControllerEndpoint setName(String v) {
            this.name = v;
            return this;
        }
        
        public ControllerEndpoint setHost(String v) {
            this.host = v;
            return this;
        }
        
        public ControllerEndpoint setPort(int v) {
            if (v < 0 || v > 65535) {
                throw new RuntimeException("Invalid value " + v + " for unsigned short field.");
            }
            this.port = v;
            return this;
        }
        
        public ControllerEndpoint setSecurityProtocol(short v) {
            this.securityProtocol = v;
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
    
    public static class ControllerEndpointCollection extends ImplicitLinkedHashMultiCollection<ControllerEndpoint> {
        public ControllerEndpointCollection() {
            super();
        }
        
        public ControllerEndpointCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public ControllerEndpointCollection(Iterator<ControllerEndpoint> iterator) {
            super(iterator);
        }
        
        public ControllerEndpoint find(String name) {
            ControllerEndpoint _key = new ControllerEndpoint();
            _key.setName(name);
            return find(_key);
        }
        
        public List<ControllerEndpoint> findAll(String name) {
            ControllerEndpoint _key = new ControllerEndpoint();
            _key.setName(name);
            return findAll(_key);
        }
        
        public ControllerEndpointCollection duplicate() {
            ControllerEndpointCollection _duplicate = new ControllerEndpointCollection(size());
            for (ControllerEndpoint _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
    
    public static class ControllerFeature implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        short minSupportedVersion;
        short maxSupportedVersion;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The feature name."),
                new Field("min_supported_version", Type.INT16, "The minimum supported feature level."),
                new Field("max_supported_version", Type.INT16, "The maximum supported feature level."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public ControllerFeature(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public ControllerFeature() {
            this.name = "";
            this.minSupportedVersion = (short) 0;
            this.maxSupportedVersion = (short) 0;
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of ControllerFeature");
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
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
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
                throw new UnsupportedVersionException("Can't size version " + _version + " of ControllerFeature");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
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
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof ControllerFeature)) return false;
            ControllerFeature other = (ControllerFeature) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ControllerFeature)) return false;
            ControllerFeature other = (ControllerFeature) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            if (minSupportedVersion != other.minSupportedVersion) return false;
            if (maxSupportedVersion != other.maxSupportedVersion) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            return hashCode;
        }
        
        @Override
        public ControllerFeature duplicate() {
            ControllerFeature _duplicate = new ControllerFeature();
            _duplicate.name = name;
            _duplicate.minSupportedVersion = minSupportedVersion;
            _duplicate.maxSupportedVersion = maxSupportedVersion;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "ControllerFeature("
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", minSupportedVersion=" + minSupportedVersion
                + ", maxSupportedVersion=" + maxSupportedVersion
                + ")";
        }
        
        public String name() {
            return this.name;
        }
        
        public short minSupportedVersion() {
            return this.minSupportedVersion;
        }
        
        public short maxSupportedVersion() {
            return this.maxSupportedVersion;
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
        
        public ControllerFeature setName(String v) {
            this.name = v;
            return this;
        }
        
        public ControllerFeature setMinSupportedVersion(short v) {
            this.minSupportedVersion = v;
            return this;
        }
        
        public ControllerFeature setMaxSupportedVersion(short v) {
            this.maxSupportedVersion = v;
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
    
    public static class ControllerFeatureCollection extends ImplicitLinkedHashMultiCollection<ControllerFeature> {
        public ControllerFeatureCollection() {
            super();
        }
        
        public ControllerFeatureCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public ControllerFeatureCollection(Iterator<ControllerFeature> iterator) {
            super(iterator);
        }
        
        public ControllerFeature find(String name) {
            ControllerFeature _key = new ControllerFeature();
            _key.setName(name);
            return find(_key);
        }
        
        public List<ControllerFeature> findAll(String name) {
            ControllerFeature _key = new ControllerFeature();
            _key.setName(name);
            return findAll(_key);
        }
        
        public ControllerFeatureCollection duplicate() {
            ControllerFeatureCollection _duplicate = new ControllerFeatureCollection(size());
            for (ControllerFeature _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
}
