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
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class GetTelemetrySubscriptionsResponseData implements ApiMessage {
    int throttleTimeMs;
    short errorCode;
    Uuid clientInstanceId;
    int subscriptionId;
    List<Byte> acceptedCompressionTypes;
    int pushIntervalMs;
    int telemetryMaxBytes;
    boolean deltaTemporality;
    List<String> requestedMetrics;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The error code, or 0 if there was no error."),
            new Field("client_instance_id", Type.UUID, "Assigned client instance id if ClientInstanceId was 0 in the request, else 0."),
            new Field("subscription_id", Type.INT32, "Unique identifier for the current subscription set for this client instance."),
            new Field("accepted_compression_types", new CompactArrayOf(Type.INT8), "Compression types that broker accepts for the PushTelemetryRequest."),
            new Field("push_interval_ms", Type.INT32, "Configured push interval, which is the lowest configured interval in the current subscription set."),
            new Field("telemetry_max_bytes", Type.INT32, "The maximum bytes of binary data the broker accepts in PushTelemetryRequest."),
            new Field("delta_temporality", Type.BOOLEAN, "Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values."),
            new Field("requested_metrics", new CompactArrayOf(Type.COMPACT_STRING), "Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public GetTelemetrySubscriptionsResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public GetTelemetrySubscriptionsResponseData() {
        this.throttleTimeMs = 0;
        this.errorCode = (short) 0;
        this.clientInstanceId = Uuid.ZERO_UUID;
        this.subscriptionId = 0;
        this.acceptedCompressionTypes = new ArrayList<Byte>(0);
        this.pushIntervalMs = 0;
        this.telemetryMaxBytes = 0;
        this.deltaTemporality = false;
        this.requestedMetrics = new ArrayList<String>(0);
    }
    
    @Override
    public short apiKey() {
        return 71;
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
        this.clientInstanceId = _readable.readUuid();
        this.subscriptionId = _readable.readInt();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field acceptedCompressionTypes was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ArrayList<Byte> newCollection = new ArrayList<>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(_readable.readByte());
                }
                this.acceptedCompressionTypes = newCollection;
            }
        }
        this.pushIntervalMs = _readable.readInt();
        this.telemetryMaxBytes = _readable.readInt();
        this.deltaTemporality = _readable.readByte() != 0;
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field requestedMetrics was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ArrayList<String> newCollection = new ArrayList<>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    int length;
                    length = _readable.readUnsignedVarint() - 1;
                    if (length < 0) {
                        throw new RuntimeException("non-nullable field requestedMetrics element was serialized as null");
                    } else if (length > 0x7fff) {
                        throw new RuntimeException("string field requestedMetrics element had invalid length " + length);
                    } else {
                        newCollection.add(_readable.readString(length));
                    }
                }
                this.requestedMetrics = newCollection;
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
        _writable.writeInt(throttleTimeMs);
        _writable.writeShort(errorCode);
        _writable.writeUuid(clientInstanceId);
        _writable.writeInt(subscriptionId);
        _writable.writeUnsignedVarint(acceptedCompressionTypes.size() + 1);
        for (Byte acceptedCompressionTypesElement : acceptedCompressionTypes) {
            _writable.writeByte(acceptedCompressionTypesElement);
        }
        _writable.writeInt(pushIntervalMs);
        _writable.writeInt(telemetryMaxBytes);
        _writable.writeByte(deltaTemporality ? (byte) 1 : (byte) 0);
        _writable.writeUnsignedVarint(requestedMetrics.size() + 1);
        for (String requestedMetricsElement : requestedMetrics) {
            {
                byte[] _stringBytes = _cache.getSerializedValue(requestedMetricsElement);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
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
        _size.addBytes(2);
        _size.addBytes(16);
        _size.addBytes(4);
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(acceptedCompressionTypes.size() + 1));
            _size.addBytes(acceptedCompressionTypes.size() * 1);
        }
        _size.addBytes(4);
        _size.addBytes(4);
        _size.addBytes(1);
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(requestedMetrics.size() + 1));
            for (String requestedMetricsElement : requestedMetrics) {
                byte[] _stringBytes = requestedMetricsElement.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'requestedMetricsElement' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(requestedMetricsElement, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
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
        if (!(obj instanceof GetTelemetrySubscriptionsResponseData)) return false;
        GetTelemetrySubscriptionsResponseData other = (GetTelemetrySubscriptionsResponseData) obj;
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (errorCode != other.errorCode) return false;
        if (!this.clientInstanceId.equals(other.clientInstanceId)) return false;
        if (subscriptionId != other.subscriptionId) return false;
        if (this.acceptedCompressionTypes == null) {
            if (other.acceptedCompressionTypes != null) return false;
        } else {
            if (!this.acceptedCompressionTypes.equals(other.acceptedCompressionTypes)) return false;
        }
        if (pushIntervalMs != other.pushIntervalMs) return false;
        if (telemetryMaxBytes != other.telemetryMaxBytes) return false;
        if (deltaTemporality != other.deltaTemporality) return false;
        if (this.requestedMetrics == null) {
            if (other.requestedMetrics != null) return false;
        } else {
            if (!this.requestedMetrics.equals(other.requestedMetrics)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + clientInstanceId.hashCode();
        hashCode = 31 * hashCode + subscriptionId;
        hashCode = 31 * hashCode + (acceptedCompressionTypes == null ? 0 : acceptedCompressionTypes.hashCode());
        hashCode = 31 * hashCode + pushIntervalMs;
        hashCode = 31 * hashCode + telemetryMaxBytes;
        hashCode = 31 * hashCode + (deltaTemporality ? 1231 : 1237);
        hashCode = 31 * hashCode + (requestedMetrics == null ? 0 : requestedMetrics.hashCode());
        return hashCode;
    }
    
    @Override
    public GetTelemetrySubscriptionsResponseData duplicate() {
        GetTelemetrySubscriptionsResponseData _duplicate = new GetTelemetrySubscriptionsResponseData();
        _duplicate.throttleTimeMs = throttleTimeMs;
        _duplicate.errorCode = errorCode;
        _duplicate.clientInstanceId = clientInstanceId;
        _duplicate.subscriptionId = subscriptionId;
        ArrayList<Byte> newAcceptedCompressionTypes = new ArrayList<Byte>(acceptedCompressionTypes.size());
        for (Byte _element : acceptedCompressionTypes) {
            newAcceptedCompressionTypes.add(_element);
        }
        _duplicate.acceptedCompressionTypes = newAcceptedCompressionTypes;
        _duplicate.pushIntervalMs = pushIntervalMs;
        _duplicate.telemetryMaxBytes = telemetryMaxBytes;
        _duplicate.deltaTemporality = deltaTemporality;
        ArrayList<String> newRequestedMetrics = new ArrayList<String>(requestedMetrics.size());
        for (String _element : requestedMetrics) {
            newRequestedMetrics.add(_element);
        }
        _duplicate.requestedMetrics = newRequestedMetrics;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "GetTelemetrySubscriptionsResponseData("
            + "throttleTimeMs=" + throttleTimeMs
            + ", errorCode=" + errorCode
            + ", clientInstanceId=" + clientInstanceId.toString()
            + ", subscriptionId=" + subscriptionId
            + ", acceptedCompressionTypes=" + MessageUtil.deepToString(acceptedCompressionTypes.iterator())
            + ", pushIntervalMs=" + pushIntervalMs
            + ", telemetryMaxBytes=" + telemetryMaxBytes
            + ", deltaTemporality=" + (deltaTemporality ? "true" : "false")
            + ", requestedMetrics=" + MessageUtil.deepToString(requestedMetrics.iterator())
            + ")";
    }
    
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public Uuid clientInstanceId() {
        return this.clientInstanceId;
    }
    
    public int subscriptionId() {
        return this.subscriptionId;
    }
    
    public List<Byte> acceptedCompressionTypes() {
        return this.acceptedCompressionTypes;
    }
    
    public int pushIntervalMs() {
        return this.pushIntervalMs;
    }
    
    public int telemetryMaxBytes() {
        return this.telemetryMaxBytes;
    }
    
    public boolean deltaTemporality() {
        return this.deltaTemporality;
    }
    
    public List<String> requestedMetrics() {
        return this.requestedMetrics;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public GetTelemetrySubscriptionsResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setClientInstanceId(Uuid v) {
        this.clientInstanceId = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setSubscriptionId(int v) {
        this.subscriptionId = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setAcceptedCompressionTypes(List<Byte> v) {
        this.acceptedCompressionTypes = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setPushIntervalMs(int v) {
        this.pushIntervalMs = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setTelemetryMaxBytes(int v) {
        this.telemetryMaxBytes = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setDeltaTemporality(boolean v) {
        this.deltaTemporality = v;
        return this;
    }
    
    public GetTelemetrySubscriptionsResponseData setRequestedMetrics(List<String> v) {
        this.requestedMetrics = v;
        return this;
    }
}
