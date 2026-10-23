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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;

import static org.apache.kafka.common.message.ConsumerGroupDescribeRequestData.*;

public class ConsumerGroupDescribeRequestDataJsonConverter {
    public static ConsumerGroupDescribeRequestData read(JsonNode _node, short _version) {
        ConsumerGroupDescribeRequestData _object = new ConsumerGroupDescribeRequestData();
        JsonNode _groupIdsNode = _node.get("groupIds");
        if (_groupIdsNode == null) {
            throw new RuntimeException("ConsumerGroupDescribeRequestData: unable to locate field 'groupIds', which is mandatory in version " + _version);
        } else {
            if (!_groupIdsNode.isArray()) {
                throw new RuntimeException("ConsumerGroupDescribeRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<String> _collection = new ArrayList<String>(_groupIdsNode.size());
            _object.groupIds = _collection;
            for (JsonNode _element : _groupIdsNode) {
                if (!_element.isTextual()) {
                    throw new RuntimeException("ConsumerGroupDescribeRequestData element expected a string type, but got " + _node.getNodeType());
                }
                _collection.add(_element.asText());
            }
        }
        JsonNode _includeAuthorizedOperationsNode = _node.get("includeAuthorizedOperations");
        if (_includeAuthorizedOperationsNode == null) {
            throw new RuntimeException("ConsumerGroupDescribeRequestData: unable to locate field 'includeAuthorizedOperations', which is mandatory in version " + _version);
        } else {
            if (!_includeAuthorizedOperationsNode.isBoolean()) {
                throw new RuntimeException("ConsumerGroupDescribeRequestData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.includeAuthorizedOperations = _includeAuthorizedOperationsNode.asBoolean();
        }
        return _object;
    }
    public static JsonNode write(ConsumerGroupDescribeRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode _groupIdsArray = new ArrayNode(JsonNodeFactory.instance);
        for (String _element : _object.groupIds) {
            _groupIdsArray.add(new TextNode(_element));
        }
        _node.set("groupIds", _groupIdsArray);
        _node.set("includeAuthorizedOperations", BooleanNode.valueOf(_object.includeAuthorizedOperations));
        return _node;
    }
    public static JsonNode write(ConsumerGroupDescribeRequestData _object, short _version) {
        return write(_object, _version, true);
    }
}
