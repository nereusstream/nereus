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
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.DescribeClusterRequestData.*;

public class DescribeClusterRequestDataJsonConverter {
    public static DescribeClusterRequestData read(JsonNode _node, short _version) {
        DescribeClusterRequestData _object = new DescribeClusterRequestData();
        JsonNode _includeClusterAuthorizedOperationsNode = _node.get("includeClusterAuthorizedOperations");
        if (_includeClusterAuthorizedOperationsNode == null) {
            throw new RuntimeException("DescribeClusterRequestData: unable to locate field 'includeClusterAuthorizedOperations', which is mandatory in version " + _version);
        } else {
            if (!_includeClusterAuthorizedOperationsNode.isBoolean()) {
                throw new RuntimeException("DescribeClusterRequestData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.includeClusterAuthorizedOperations = _includeClusterAuthorizedOperationsNode.asBoolean();
        }
        JsonNode _endpointTypeNode = _node.get("endpointType");
        if (_endpointTypeNode == null) {
            if (_version >= 1) {
                throw new RuntimeException("DescribeClusterRequestData: unable to locate field 'endpointType', which is mandatory in version " + _version);
            } else {
                _object.endpointType = (byte) 1;
            }
        } else {
            _object.endpointType = MessageUtil.jsonNodeToByte(_endpointTypeNode, "DescribeClusterRequestData");
        }
        JsonNode _includeFencedBrokersNode = _node.get("includeFencedBrokers");
        if (_includeFencedBrokersNode == null) {
            if (_version >= 2) {
                throw new RuntimeException("DescribeClusterRequestData: unable to locate field 'includeFencedBrokers', which is mandatory in version " + _version);
            } else {
                _object.includeFencedBrokers = false;
            }
        } else {
            if (!_includeFencedBrokersNode.isBoolean()) {
                throw new RuntimeException("DescribeClusterRequestData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.includeFencedBrokers = _includeFencedBrokersNode.asBoolean();
        }
        return _object;
    }
    public static JsonNode write(DescribeClusterRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("includeClusterAuthorizedOperations", BooleanNode.valueOf(_object.includeClusterAuthorizedOperations));
        if (_version >= 1) {
            _node.set("endpointType", new ShortNode(_object.endpointType));
        } else {
            if (_object.endpointType != (byte) 1) {
                throw new UnsupportedVersionException("Attempted to write a non-default endpointType at version " + _version);
            }
        }
        if (_version >= 2) {
            _node.set("includeFencedBrokers", BooleanNode.valueOf(_object.includeFencedBrokers));
        } else {
            if (_object.includeFencedBrokers) {
                throw new UnsupportedVersionException("Attempted to write a non-default includeFencedBrokers at version " + _version);
            }
        }
        return _node;
    }
    public static JsonNode write(DescribeClusterRequestData _object, short _version) {
        return write(_object, _version, true);
    }
}
