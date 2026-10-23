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


public class ControllerRegistrationRequestData implements ApiMessage {
    int controllerId;
    Uuid incarnationId;
    boolean zkMigrationReady;
    ListenerCollection listeners;
    FeatureCollection features;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("controller_id", Type.INT32, "The ID of the controller to register."),
            new Field("incarnation_id", Type.UUID, "The controller incarnation ID, which is unique to each process run."),
            new Field("zk_migration_ready", Type.BOOLEAN, "Set if the required configurations for ZK migration are present."),
            new Field("listeners", new CompactArrayOf(Listener.SCHEMA_0), "The listeners of this controller."),
            new Field("features", new CompactArrayOf(Feature.SCHEMA_0), "The features on this controller."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public ControllerRegistrationRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public ControllerRegistrationRequestData() {
        this.controllerId = 0;
        this.incarnationId = Uuid.ZERO_UUID;
        this.zkMigrationReady = false;
        this.listeners = new ListenerCollection(0);
        this.features = new FeatureCollection(0);
    }
    
    @Override
    public short apiKey() {
        return 70;
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
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field features was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                FeatureCollection newCollection = new FeatureCollection(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new Feature(_readable, _version));
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
        _writable.writeUnsignedVarint(listeners.size() + 1);
        for (Listener listenersElement : listeners) {
            listenersElement.write(_writable, _cache, _version);
        }
        _writable.writeUnsignedVarint(features.size() + 1);
        for (Feature featuresElement : features) {
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
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(listeners.size() + 1));
            for (Listener listenersElement : listeners) {
                listenersElement.addSize(_size, _cache, _version);
            }
        }
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(features.size() + 1));
            for (Feature featuresElement : features) {
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
        if (!(obj instanceof ControllerRegistrationRequestData)) return false;
        ControllerRegistrationRequestData other = (ControllerRegistrationRequestData) obj;
        if (controllerId != other.controllerId) return false;
        if (!this.incarnationId.equals(other.incarnationId)) return false;
        if (zkMigrationReady != other.zkMigrationReady) return false;
        if (this.listeners == null) {
            if (other.listeners != null) return false;
        } else {
            if (!this.listeners.equals(other.listeners)) return false;
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
        hashCode = 31 * hashCode + (listeners == null ? 0 : listeners.hashCode());
        hashCode = 31 * hashCode + (features == null ? 0 : features.hashCode());
        return hashCode;
    }
    
    @Override
    public ControllerRegistrationRequestData duplicate() {
        ControllerRegistrationRequestData _duplicate = new ControllerRegistrationRequestData();
        _duplicate.controllerId = controllerId;
        _duplicate.incarnationId = incarnationId;
        _duplicate.zkMigrationReady = zkMigrationReady;
        ListenerCollection newListeners = new ListenerCollection(listeners.size());
        for (Listener _element : listeners) {
            newListeners.add(_element.duplicate());
        }
        _duplicate.listeners = newListeners;
        FeatureCollection newFeatures = new FeatureCollection(features.size());
        for (Feature _element : features) {
            newFeatures.add(_element.duplicate());
        }
        _duplicate.features = newFeatures;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "ControllerRegistrationRequestData("
            + "controllerId=" + controllerId
            + ", incarnationId=" + incarnationId.toString()
            + ", zkMigrationReady=" + (zkMigrationReady ? "true" : "false")
            + ", listeners=" + MessageUtil.deepToString(listeners.iterator())
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
    
    public ListenerCollection listeners() {
        return this.listeners;
    }
    
    public FeatureCollection features() {
        return this.features;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public ControllerRegistrationRequestData setControllerId(int v) {
        this.controllerId = v;
        return this;
    }
    
    public ControllerRegistrationRequestData setIncarnationId(Uuid v) {
        this.incarnationId = v;
        return this;
    }
    
    public ControllerRegistrationRequestData setZkMigrationReady(boolean v) {
        this.zkMigrationReady = v;
        return this;
    }
    
    public ControllerRegistrationRequestData setListeners(ListenerCollection v) {
        this.listeners = v;
        return this;
    }
    
    public ControllerRegistrationRequestData setFeatures(FeatureCollection v) {
        this.features = v;
        return this;
    }
    
    public static class Listener implements Message, ImplicitLinkedHashMultiCollection.Element {
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
        
        public Listener(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public Listener() {
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
        public Listener duplicate() {
            Listener _duplicate = new Listener();
            _duplicate.name = name;
            _duplicate.host = host;
            _duplicate.port = port;
            _duplicate.securityProtocol = securityProtocol;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Listener("
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
        
        public Listener setSecurityProtocol(short v) {
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
    
    public static class Feature implements Message, ImplicitLinkedHashMultiCollection.Element {
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
        
        public Feature(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public Feature() {
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
                throw new UnsupportedVersionException("Can't read version " + _version + " of Feature");
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
                throw new UnsupportedVersionException("Can't size version " + _version + " of Feature");
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
            if (!(obj instanceof Feature)) return false;
            Feature other = (Feature) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Feature)) return false;
            Feature other = (Feature) obj;
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
        public Feature duplicate() {
            Feature _duplicate = new Feature();
            _duplicate.name = name;
            _duplicate.minSupportedVersion = minSupportedVersion;
            _duplicate.maxSupportedVersion = maxSupportedVersion;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Feature("
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
        
        public Feature setName(String v) {
            this.name = v;
            return this;
        }
        
        public Feature setMinSupportedVersion(short v) {
            this.minSupportedVersion = v;
            return this;
        }
        
        public Feature setMaxSupportedVersion(short v) {
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
    
    public static class FeatureCollection extends ImplicitLinkedHashMultiCollection<Feature> {
        public FeatureCollection() {
            super();
        }
        
        public FeatureCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public FeatureCollection(Iterator<Feature> iterator) {
            super(iterator);
        }
        
        public Feature find(String name) {
            Feature _key = new Feature();
            _key.setName(name);
            return find(_key);
        }
        
        public List<Feature> findAll(String name) {
            Feature _key = new Feature();
            _key.setName(name);
            return findAll(_key);
        }
        
        public FeatureCollection duplicate() {
            FeatureCollection _duplicate = new FeatureCollection(size());
            for (Feature _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
}
