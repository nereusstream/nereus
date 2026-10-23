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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData.*;

public class GetTelemetrySubscriptionsResponseDataJsonConverter {
    public static GetTelemetrySubscriptionsResponseData read(JsonNode _node, short _version) {
        GetTelemetrySubscriptionsResponseData _object = new GetTelemetrySubscriptionsResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "GetTelemetrySubscriptionsResponseData");
        }
        JsonNode _errorCodeNode = _node.get("errorCode");
        if (_errorCodeNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'errorCode', which is mandatory in version " + _version);
        } else {
            _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "GetTelemetrySubscriptionsResponseData");
        }
        JsonNode _clientInstanceIdNode = _node.get("clientInstanceId");
        if (_clientInstanceIdNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'clientInstanceId', which is mandatory in version " + _version);
        } else {
            if (!_clientInstanceIdNode.isTextual()) {
                throw new RuntimeException("GetTelemetrySubscriptionsResponseData expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.clientInstanceId = Uuid.fromString(_clientInstanceIdNode.asText());
        }
        JsonNode _subscriptionIdNode = _node.get("subscriptionId");
        if (_subscriptionIdNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'subscriptionId', which is mandatory in version " + _version);
        } else {
            _object.subscriptionId = MessageUtil.jsonNodeToInt(_subscriptionIdNode, "GetTelemetrySubscriptionsResponseData");
        }
        JsonNode _acceptedCompressionTypesNode = _node.get("acceptedCompressionTypes");
        if (_acceptedCompressionTypesNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'acceptedCompressionTypes', which is mandatory in version " + _version);
        } else {
            if (!_acceptedCompressionTypesNode.isArray()) {
                throw new RuntimeException("GetTelemetrySubscriptionsResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<Byte> _collection = new ArrayList<Byte>(_acceptedCompressionTypesNode.size());
            _object.acceptedCompressionTypes = _collection;
            for (JsonNode _element : _acceptedCompressionTypesNode) {
                _collection.add(MessageUtil.jsonNodeToByte(_element, "GetTelemetrySubscriptionsResponseData element"));
            }
        }
        JsonNode _pushIntervalMsNode = _node.get("pushIntervalMs");
        if (_pushIntervalMsNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'pushIntervalMs', which is mandatory in version " + _version);
        } else {
            _object.pushIntervalMs = MessageUtil.jsonNodeToInt(_pushIntervalMsNode, "GetTelemetrySubscriptionsResponseData");
        }
        JsonNode _telemetryMaxBytesNode = _node.get("telemetryMaxBytes");
        if (_telemetryMaxBytesNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'telemetryMaxBytes', which is mandatory in version " + _version);
        } else {
            _object.telemetryMaxBytes = MessageUtil.jsonNodeToInt(_telemetryMaxBytesNode, "GetTelemetrySubscriptionsResponseData");
        }
        JsonNode _deltaTemporalityNode = _node.get("deltaTemporality");
        if (_deltaTemporalityNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'deltaTemporality', which is mandatory in version " + _version);
        } else {
            if (!_deltaTemporalityNode.isBoolean()) {
                throw new RuntimeException("GetTelemetrySubscriptionsResponseData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.deltaTemporality = _deltaTemporalityNode.asBoolean();
        }
        JsonNode _requestedMetricsNode = _node.get("requestedMetrics");
        if (_requestedMetricsNode == null) {
            throw new RuntimeException("GetTelemetrySubscriptionsResponseData: unable to locate field 'requestedMetrics', which is mandatory in version " + _version);
        } else {
            if (!_requestedMetricsNode.isArray()) {
                throw new RuntimeException("GetTelemetrySubscriptionsResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<String> _collection = new ArrayList<String>(_requestedMetricsNode.size());
            _object.requestedMetrics = _collection;
            for (JsonNode _element : _requestedMetricsNode) {
                if (!_element.isTextual()) {
                    throw new RuntimeException("GetTelemetrySubscriptionsResponseData element expected a string type, but got " + _node.getNodeType());
                }
                _collection.add(_element.asText());
            }
        }
        return _object;
    }
    public static JsonNode write(GetTelemetrySubscriptionsResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        _node.set("errorCode", new ShortNode(_object.errorCode));
        _node.set("clientInstanceId", new TextNode(_object.clientInstanceId.toString()));
        _node.set("subscriptionId", new IntNode(_object.subscriptionId));
        ArrayNode _acceptedCompressionTypesArray = new ArrayNode(JsonNodeFactory.instance);
        for (Byte _element : _object.acceptedCompressionTypes) {
            _acceptedCompressionTypesArray.add(new ShortNode(_element));
        }
        _node.set("acceptedCompressionTypes", _acceptedCompressionTypesArray);
        _node.set("pushIntervalMs", new IntNode(_object.pushIntervalMs));
        _node.set("telemetryMaxBytes", new IntNode(_object.telemetryMaxBytes));
        _node.set("deltaTemporality", BooleanNode.valueOf(_object.deltaTemporality));
        ArrayNode _requestedMetricsArray = new ArrayNode(JsonNodeFactory.instance);
        for (String _element : _object.requestedMetrics) {
            _requestedMetricsArray.add(new TextNode(_element));
        }
        _node.set("requestedMetrics", _requestedMetricsArray);
        return _node;
    }
    public static JsonNode write(GetTelemetrySubscriptionsResponseData _object, short _version) {
        return write(_object, _version, true);
    }
}
