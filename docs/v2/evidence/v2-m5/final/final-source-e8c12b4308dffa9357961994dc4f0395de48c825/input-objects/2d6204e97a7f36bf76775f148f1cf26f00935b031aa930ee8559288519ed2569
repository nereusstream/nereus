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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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


public class PushTelemetryRequestData implements ApiMessage {
    Uuid clientInstanceId;
    int subscriptionId;
    boolean terminating;
    byte compressionType;
    ByteBuffer metrics;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("client_instance_id", Type.UUID, "Unique id for this client instance."),
            new Field("subscription_id", Type.INT32, "Unique identifier for the current subscription."),
            new Field("terminating", Type.BOOLEAN, "Client is terminating the connection."),
            new Field("compression_type", Type.INT8, "Compression codec used to compress the metrics."),
            new Field("metrics", Type.COMPACT_BYTES, "Metrics encoded in OpenTelemetry MetricsData v1 protobuf format."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public PushTelemetryRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public PushTelemetryRequestData() {
        this.clientInstanceId = Uuid.ZERO_UUID;
        this.subscriptionId = 0;
        this.terminating = false;
        this.compressionType = (byte) 0;
        this.metrics = ByteUtils.EMPTY_BUF;
    }
    
    @Override
    public short apiKey() {
        return 72;
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
        this.clientInstanceId = _readable.readUuid();
        this.subscriptionId = _readable.readInt();
        this.terminating = _readable.readByte() != 0;
        this.compressionType = _readable.readByte();
        {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                throw new RuntimeException("non-nullable field metrics was serialized as null");
            } else {
                this.metrics = _readable.readByteBuffer(length);
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
        _writable.writeUuid(clientInstanceId);
        _writable.writeInt(subscriptionId);
        _writable.writeByte(terminating ? (byte) 1 : (byte) 0);
        _writable.writeByte(compressionType);
        _writable.writeUnsignedVarint(metrics.remaining() + 1);
        _writable.writeByteBuffer(metrics);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(16);
        _size.addBytes(4);
        _size.addBytes(1);
        _size.addBytes(1);
        {
            _size.addZeroCopyBytes(metrics.remaining());
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(metrics.remaining() + 1));
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
        if (!(obj instanceof PushTelemetryRequestData)) return false;
        PushTelemetryRequestData other = (PushTelemetryRequestData) obj;
        if (!this.clientInstanceId.equals(other.clientInstanceId)) return false;
        if (subscriptionId != other.subscriptionId) return false;
        if (terminating != other.terminating) return false;
        if (compressionType != other.compressionType) return false;
        if (!Objects.equals(this.metrics, other.metrics)) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + clientInstanceId.hashCode();
        hashCode = 31 * hashCode + subscriptionId;
        hashCode = 31 * hashCode + (terminating ? 1231 : 1237);
        hashCode = 31 * hashCode + compressionType;
        hashCode = 31 * hashCode + Objects.hashCode(metrics);
        return hashCode;
    }
    
    @Override
    public PushTelemetryRequestData duplicate() {
        PushTelemetryRequestData _duplicate = new PushTelemetryRequestData();
        _duplicate.clientInstanceId = clientInstanceId;
        _duplicate.subscriptionId = subscriptionId;
        _duplicate.terminating = terminating;
        _duplicate.compressionType = compressionType;
        _duplicate.metrics = metrics.duplicate();
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "PushTelemetryRequestData("
            + "clientInstanceId=" + clientInstanceId.toString()
            + ", subscriptionId=" + subscriptionId
            + ", terminating=" + (terminating ? "true" : "false")
            + ", compressionType=" + compressionType
            + ", metrics=" + metrics
            + ")";
    }
    
    public Uuid clientInstanceId() {
        return this.clientInstanceId;
    }
    
    public int subscriptionId() {
        return this.subscriptionId;
    }
    
    public boolean terminating() {
        return this.terminating;
    }
    
    public byte compressionType() {
        return this.compressionType;
    }
    
    public ByteBuffer metrics() {
        return this.metrics;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public PushTelemetryRequestData setClientInstanceId(Uuid v) {
        this.clientInstanceId = v;
        return this;
    }
    
    public PushTelemetryRequestData setSubscriptionId(int v) {
        this.subscriptionId = v;
        return this;
    }
    
    public PushTelemetryRequestData setTerminating(boolean v) {
        this.terminating = v;
        return this;
    }
    
    public PushTelemetryRequestData setCompressionType(byte v) {
        this.compressionType = v;
        return this;
    }
    
    public PushTelemetryRequestData setMetrics(ByteBuffer v) {
        this.metrics = v;
        return this;
    }
}
