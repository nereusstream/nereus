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
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.nio.ByteBuffer;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.PushTelemetryRequestData.*;

public class PushTelemetryRequestDataJsonConverter {
    public static PushTelemetryRequestData read(JsonNode _node, short _version) {
        PushTelemetryRequestData _object = new PushTelemetryRequestData();
        JsonNode _clientInstanceIdNode = _node.get("clientInstanceId");
        if (_clientInstanceIdNode == null) {
            throw new RuntimeException("PushTelemetryRequestData: unable to locate field 'clientInstanceId', which is mandatory in version " + _version);
        } else {
            if (!_clientInstanceIdNode.isTextual()) {
                throw new RuntimeException("PushTelemetryRequestData expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.clientInstanceId = Uuid.fromString(_clientInstanceIdNode.asText());
        }
        JsonNode _subscriptionIdNode = _node.get("subscriptionId");
        if (_subscriptionIdNode == null) {
            throw new RuntimeException("PushTelemetryRequestData: unable to locate field 'subscriptionId', which is mandatory in version " + _version);
        } else {
            _object.subscriptionId = MessageUtil.jsonNodeToInt(_subscriptionIdNode, "PushTelemetryRequestData");
        }
        JsonNode _terminatingNode = _node.get("terminating");
        if (_terminatingNode == null) {
            throw new RuntimeException("PushTelemetryRequestData: unable to locate field 'terminating', which is mandatory in version " + _version);
        } else {
            if (!_terminatingNode.isBoolean()) {
                throw new RuntimeException("PushTelemetryRequestData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.terminating = _terminatingNode.asBoolean();
        }
        JsonNode _compressionTypeNode = _node.get("compressionType");
        if (_compressionTypeNode == null) {
            throw new RuntimeException("PushTelemetryRequestData: unable to locate field 'compressionType', which is mandatory in version " + _version);
        } else {
            _object.compressionType = MessageUtil.jsonNodeToByte(_compressionTypeNode, "PushTelemetryRequestData");
        }
        JsonNode _metricsNode = _node.get("metrics");
        if (_metricsNode == null) {
            throw new RuntimeException("PushTelemetryRequestData: unable to locate field 'metrics', which is mandatory in version " + _version);
        } else {
            _object.metrics = ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(_metricsNode, "PushTelemetryRequestData"));
        }
        return _object;
    }
    public static JsonNode write(PushTelemetryRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("clientInstanceId", new TextNode(_object.clientInstanceId.toString()));
        _node.set("subscriptionId", new IntNode(_object.subscriptionId));
        _node.set("terminating", BooleanNode.valueOf(_object.terminating));
        _node.set("compressionType", new ShortNode(_object.compressionType));
        _node.set("metrics", new BinaryNode(MessageUtil.byteBufferToArray(_object.metrics)));
        return _node;
    }
    public static JsonNode write(PushTelemetryRequestData _object, short _version) {
        return write(_object, _version, true);
    }
}
