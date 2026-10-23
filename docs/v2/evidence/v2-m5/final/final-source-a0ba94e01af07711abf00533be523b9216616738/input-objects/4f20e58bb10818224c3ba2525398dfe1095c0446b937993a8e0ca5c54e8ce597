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

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
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


public class BrokerRegistrationChangeRecord implements ApiMessage {
    int brokerId;
    long brokerEpoch;
    byte fenced;
    byte inControlledShutdown;
    List<Uuid> logDirs;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("broker_id", Type.INT32, "The broker id."),
            new Field("broker_epoch", Type.INT64, "The broker epoch assigned by the controller."),
            TaggedFieldsSection.of(
                0, new Field("fenced", Type.INT8, "-1 if the broker has been unfenced, 0 if no change, 1 if the broker has been fenced.")
            )
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("broker_id", Type.INT32, "The broker id."),
            new Field("broker_epoch", Type.INT64, "The broker epoch assigned by the controller."),
            TaggedFieldsSection.of(
                0, new Field("fenced", Type.INT8, "-1 if the broker has been unfenced, 0 if no change, 1 if the broker has been fenced."),
                1, new Field("in_controlled_shutdown", Type.INT8, "0 if no change, 1 if the broker is in controlled shutdown.")
            )
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("broker_id", Type.INT32, "The broker id."),
            new Field("broker_epoch", Type.INT64, "The broker epoch assigned by the controller."),
            TaggedFieldsSection.of(
                0, new Field("fenced", Type.INT8, "-1 if the broker has been unfenced, 0 if no change, 1 if the broker has been fenced."),
                1, new Field("in_controlled_shutdown", Type.INT8, "0 if no change, 1 if the broker is in controlled shutdown."),
                2, new Field("log_dirs", new CompactArrayOf(Type.UUID), "Log directories configured in this broker which are available.")
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 2;
    
    public BrokerRegistrationChangeRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public BrokerRegistrationChangeRecord() {
        this.brokerId = 0;
        this.brokerEpoch = 0L;
        this.fenced = (byte) 0;
        this.inControlledShutdown = (byte) 0;
        this.logDirs = new ArrayList<Uuid>(0);
    }
    
    @Override
    public short apiKey() {
        return 17;
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
        this.brokerId = _readable.readInt();
        this.brokerEpoch = _readable.readLong();
        this.fenced = (byte) 0;
        this.inControlledShutdown = (byte) 0;
        {
            this.logDirs = new ArrayList<Uuid>(0);
        }
        this._unknownTaggedFields = null;
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; _i++) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                case 0: {
                    this.fenced = _readable.readByte();
                    break;
                }
                case 1: {
                    if (_version >= 1) {
                        this.inControlledShutdown = _readable.readByte();
                        break;
                    } else {
                        throw new RuntimeException("Tag 1 is not valid for version " + _version);
                    }
                }
                case 2: {
                    if (_version >= 2) {
                        int arrayLength;
                        arrayLength = _readable.readUnsignedVarint() - 1;
                        if (arrayLength < 0) {
                            throw new RuntimeException("non-nullable field logDirs was serialized as null");
                        } else {
                            if (arrayLength > _readable.remaining()) {
                                throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                            }
                            ArrayList<Uuid> newCollection = new ArrayList<>(arrayLength);
                            for (int i = 0; i < arrayLength; i++) {
                                newCollection.add(_readable.readUuid());
                            }
                            this.logDirs = newCollection;
                        }
                        break;
                    } else {
                        throw new RuntimeException("Tag 2 is not valid for version " + _version);
                    }
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
        _writable.writeInt(brokerId);
        _writable.writeLong(brokerEpoch);
        if (this.fenced != (byte) 0) {
            _numTaggedFields++;
        }
        if (_version >= 1) {
            if (this.inControlledShutdown != (byte) 0) {
                _numTaggedFields++;
            }
        } else {
            if (this.inControlledShutdown != (byte) 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default inControlledShutdown at version " + _version);
            }
        }
        if (_version >= 2) {
            if (!this.logDirs.isEmpty()) {
                _numTaggedFields++;
            }
        } else {
            if (!this.logDirs.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default logDirs at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        {
            if (this.fenced != (byte) 0) {
                _writable.writeUnsignedVarint(0);
                _writable.writeUnsignedVarint(1);
                _writable.writeByte(fenced);
            }
        }
        if (_version >= 1) {
            {
                if (this.inControlledShutdown != (byte) 0) {
                    _writable.writeUnsignedVarint(1);
                    _writable.writeUnsignedVarint(1);
                    _writable.writeByte(inControlledShutdown);
                }
            }
        }
        if (_version >= 2) {
            {
                if (!this.logDirs.isEmpty()) {
                    _writable.writeUnsignedVarint(2);
                    _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.logDirs));
                    _writable.writeUnsignedVarint(logDirs.size() + 1);
                    for (Uuid logDirsElement : logDirs) {
                        _writable.writeUuid(logDirsElement);
                    }
                }
            }
        }
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(4);
        _size.addBytes(8);
        if (this.fenced != (byte) 0) {
            _numTaggedFields++;
            _size.addBytes(1);
            _size.addBytes(1);
            _size.addBytes(1);
        }
        if (_version >= 1) {
            if (this.inControlledShutdown != (byte) 0) {
                _numTaggedFields++;
                _size.addBytes(1);
                _size.addBytes(1);
                _size.addBytes(1);
            }
        }
        if (_version >= 2) {
            {
                if (!this.logDirs.isEmpty()) {
                    _numTaggedFields++;
                    _size.addBytes(1);
                    int _sizeBeforeArray = _size.totalSize();
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(logDirs.size() + 1));
                    _size.addBytes(logDirs.size() * 16);
                    int _arraySize = _size.totalSize() - _sizeBeforeArray;
                    _cache.setArraySizeInBytes(logDirs, _arraySize);
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
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
        if (!(obj instanceof BrokerRegistrationChangeRecord)) return false;
        BrokerRegistrationChangeRecord other = (BrokerRegistrationChangeRecord) obj;
        if (brokerId != other.brokerId) return false;
        if (brokerEpoch != other.brokerEpoch) return false;
        if (fenced != other.fenced) return false;
        if (inControlledShutdown != other.inControlledShutdown) return false;
        if (this.logDirs == null) {
            if (other.logDirs != null) return false;
        } else {
            if (!this.logDirs.equals(other.logDirs)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + brokerId;
        hashCode = 31 * hashCode + ((int) (brokerEpoch >> 32) ^ (int) brokerEpoch);
        hashCode = 31 * hashCode + fenced;
        hashCode = 31 * hashCode + inControlledShutdown;
        hashCode = 31 * hashCode + (logDirs == null ? 0 : logDirs.hashCode());
        return hashCode;
    }
    
    @Override
    public BrokerRegistrationChangeRecord duplicate() {
        BrokerRegistrationChangeRecord _duplicate = new BrokerRegistrationChangeRecord();
        _duplicate.brokerId = brokerId;
        _duplicate.brokerEpoch = brokerEpoch;
        _duplicate.fenced = fenced;
        _duplicate.inControlledShutdown = inControlledShutdown;
        ArrayList<Uuid> newLogDirs = new ArrayList<Uuid>(logDirs.size());
        for (Uuid _element : logDirs) {
            newLogDirs.add(_element);
        }
        _duplicate.logDirs = newLogDirs;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "BrokerRegistrationChangeRecord("
            + "brokerId=" + brokerId
            + ", brokerEpoch=" + brokerEpoch
            + ", fenced=" + fenced
            + ", inControlledShutdown=" + inControlledShutdown
            + ", logDirs=" + MessageUtil.deepToString(logDirs.iterator())
            + ")";
    }
    
    public int brokerId() {
        return this.brokerId;
    }
    
    public long brokerEpoch() {
        return this.brokerEpoch;
    }
    
    public byte fenced() {
        return this.fenced;
    }
    
    public byte inControlledShutdown() {
        return this.inControlledShutdown;
    }
    
    public List<Uuid> logDirs() {
        return this.logDirs;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public BrokerRegistrationChangeRecord setBrokerId(int v) {
        this.brokerId = v;
        return this;
    }
    
    public BrokerRegistrationChangeRecord setBrokerEpoch(long v) {
        this.brokerEpoch = v;
        return this;
    }
    
    public BrokerRegistrationChangeRecord setFenced(byte v) {
        this.fenced = v;
        return this;
    }
    
    public BrokerRegistrationChangeRecord setInControlledShutdown(byte v) {
        this.inControlledShutdown = v;
        return this;
    }
    
    public BrokerRegistrationChangeRecord setLogDirs(List<Uuid> v) {
        this.logDirs = v;
        return this;
    }
}
