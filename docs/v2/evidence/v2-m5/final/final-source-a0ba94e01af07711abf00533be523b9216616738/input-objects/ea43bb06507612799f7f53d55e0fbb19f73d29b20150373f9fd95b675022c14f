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
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.DeleteShareGroupStateResponseData.*;

public class DeleteShareGroupStateResponseDataJsonConverter {
    public static DeleteShareGroupStateResponseData read(JsonNode _node, short _version) {
        DeleteShareGroupStateResponseData _object = new DeleteShareGroupStateResponseData();
        JsonNode _resultsNode = _node.get("results");
        if (_resultsNode == null) {
            throw new RuntimeException("DeleteShareGroupStateResponseData: unable to locate field 'results', which is mandatory in version " + _version);
        } else {
            if (!_resultsNode.isArray()) {
                throw new RuntimeException("DeleteShareGroupStateResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<DeleteStateResult> _collection = new ArrayList<DeleteStateResult>(_resultsNode.size());
            _object.results = _collection;
            for (JsonNode _element : _resultsNode) {
                _collection.add(DeleteStateResultJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(DeleteShareGroupStateResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode _resultsArray = new ArrayNode(JsonNodeFactory.instance);
        for (DeleteStateResult _element : _object.results) {
            _resultsArray.add(DeleteStateResultJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("results", _resultsArray);
        return _node;
    }
    public static JsonNode write(DeleteShareGroupStateResponseData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class DeleteStateResultJsonConverter {
        public static DeleteStateResult read(JsonNode _node, short _version) {
            DeleteStateResult _object = new DeleteStateResult();
            JsonNode _topicIdNode = _node.get("topicId");
            if (_topicIdNode == null) {
                throw new RuntimeException("DeleteStateResult: unable to locate field 'topicId', which is mandatory in version " + _version);
            } else {
                if (!_topicIdNode.isTextual()) {
                    throw new RuntimeException("DeleteStateResult expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.topicId = Uuid.fromString(_topicIdNode.asText());
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("DeleteStateResult: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("DeleteStateResult expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<PartitionResult> _collection = new ArrayList<PartitionResult>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(PartitionResultJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(DeleteStateResult _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("topicId", new TextNode(_object.topicId.toString()));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (PartitionResult _element : _object.partitions) {
                _partitionsArray.add(PartitionResultJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(DeleteStateResult _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class PartitionResultJsonConverter {
        public static PartitionResult read(JsonNode _node, short _version) {
            PartitionResult _object = new PartitionResult();
            JsonNode _partitionNode = _node.get("partition");
            if (_partitionNode == null) {
                throw new RuntimeException("PartitionResult: unable to locate field 'partition', which is mandatory in version " + _version);
            } else {
                _object.partition = MessageUtil.jsonNodeToInt(_partitionNode, "PartitionResult");
            }
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("PartitionResult: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "PartitionResult");
            }
            JsonNode _errorMessageNode = _node.get("errorMessage");
            if (_errorMessageNode == null) {
                throw new RuntimeException("PartitionResult: unable to locate field 'errorMessage', which is mandatory in version " + _version);
            } else {
                if (_errorMessageNode.isNull()) {
                    _object.errorMessage = null;
                } else {
                    if (!_errorMessageNode.isTextual()) {
                        throw new RuntimeException("PartitionResult expected a string type, but got " + _node.getNodeType());
                    }
                    _object.errorMessage = _errorMessageNode.asText();
                }
            }
            return _object;
        }
        public static JsonNode write(PartitionResult _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partition", new IntNode(_object.partition));
            _node.set("errorCode", new ShortNode(_object.errorCode));
            if (_object.errorMessage == null) {
                _node.set("errorMessage", NullNode.instance);
            } else {
                _node.set("errorMessage", new TextNode(_object.errorMessage));
            }
            return _node;
        }
        public static JsonNode write(PartitionResult _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
