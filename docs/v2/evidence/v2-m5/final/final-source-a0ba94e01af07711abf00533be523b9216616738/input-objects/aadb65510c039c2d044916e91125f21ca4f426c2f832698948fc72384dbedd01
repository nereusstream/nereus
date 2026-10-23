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
import java.util.List;
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


public class AbortTransactionRecord implements ApiMessage {
    String reason;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            TaggedFieldsSection.of(
                0, new Field("reason", Type.COMPACT_NULLABLE_STRING, "An optional textual description of why the transaction was aborted.")
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public AbortTransactionRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public AbortTransactionRecord() {
        this.reason = null;
    }
    
    @Override
    public short apiKey() {
        return 25;
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
            this.reason = null;
        }
        this._unknownTaggedFields = null;
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; _i++) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                case 0: {
                    int length;
                    length = _readable.readUnsignedVarint() - 1;
                    if (length < 0) {
                        this.reason = null;
                    } else if (length > 0x7fff) {
                        throw new RuntimeException("string field reason had invalid length " + length);
                    } else {
                        this.reason = _readable.readString(length);
                    }
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
        if (this.reason != null) {
            _numTaggedFields++;
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        if (reason != null) {
            _writable.writeUnsignedVarint(0);
            byte[] _stringBytes = _cache.getSerializedValue(this.reason);
            _writable.writeUnsignedVarint(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            _writable.writeUnsignedVarint(_stringBytes.length + 1);
            _writable.writeByteArray(_stringBytes);
        }
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (reason == null) {
        } else {
            _numTaggedFields++;
            _size.addBytes(1);
            byte[] _stringBytes = reason.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'reason' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(reason, _stringBytes);
            int _stringPrefixSize = ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
            _size.addBytes(_stringBytes.length + _stringPrefixSize + ByteUtils.sizeOfUnsignedVarint(_stringPrefixSize + _stringBytes.length));
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
        if (!(obj instanceof AbortTransactionRecord)) return false;
        AbortTransactionRecord other = (AbortTransactionRecord) obj;
        if (this.reason == null) {
            if (other.reason != null) return false;
        } else {
            if (!this.reason.equals(other.reason)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (reason == null ? 0 : reason.hashCode());
        return hashCode;
    }
    
    @Override
    public AbortTransactionRecord duplicate() {
        AbortTransactionRecord _duplicate = new AbortTransactionRecord();
        if (reason == null) {
            _duplicate.reason = null;
        } else {
            _duplicate.reason = reason;
        }
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "AbortTransactionRecord("
            + "reason=" + ((reason == null) ? "null" : "'" + reason.toString() + "'")
            + ")";
    }
    
    public String reason() {
        return this.reason;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public AbortTransactionRecord setReason(String v) {
        this.reason = v;
        return this;
    }
}
