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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.DeleteShareGroupStateRequestData.*;

public class DeleteShareGroupStateRequestDataJsonConverter {
    public static DeleteShareGroupStateRequestData read(JsonNode _node, short _version) {
        DeleteShareGroupStateRequestData _object = new DeleteShareGroupStateRequestData();
        JsonNode _groupIdNode = _node.get("groupId");
        if (_groupIdNode == null) {
            throw new RuntimeException("DeleteShareGroupStateRequestData: unable to locate field 'groupId', which is mandatory in version " + _version);
        } else {
            if (!_groupIdNode.isTextual()) {
                throw new RuntimeException("DeleteShareGroupStateRequestData expected a string type, but got " + _node.getNodeType());
            }
            _object.groupId = _groupIdNode.asText();
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("DeleteShareGroupStateRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("DeleteShareGroupStateRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<DeleteStateData> _collection = new ArrayList<DeleteStateData>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(DeleteStateDataJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(DeleteShareGroupStateRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("groupId", new TextNode(_object.groupId));
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (DeleteStateData _element : _object.topics) {
            _topicsArray.add(DeleteStateDataJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    public static JsonNode write(DeleteShareGroupStateRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class DeleteStateDataJsonConverter {
        public static DeleteStateData read(JsonNode _node, short _version) {
            DeleteStateData _object = new DeleteStateData();
            JsonNode _topicIdNode = _node.get("topicId");
            if (_topicIdNode == null) {
                throw new RuntimeException("DeleteStateData: unable to locate field 'topicId', which is mandatory in version " + _version);
            } else {
                if (!_topicIdNode.isTextual()) {
                    throw new RuntimeException("DeleteStateData expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.topicId = Uuid.fromString(_topicIdNode.asText());
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("DeleteStateData: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("DeleteStateData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<PartitionData> _collection = new ArrayList<PartitionData>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(PartitionDataJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(DeleteStateData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("topicId", new TextNode(_object.topicId.toString()));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (PartitionData _element : _object.partitions) {
                _partitionsArray.add(PartitionDataJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(DeleteStateData _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class PartitionDataJsonConverter {
        public static PartitionData read(JsonNode _node, short _version) {
            PartitionData _object = new PartitionData();
            JsonNode _partitionNode = _node.get("partition");
            if (_partitionNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'partition', which is mandatory in version " + _version);
            } else {
                _object.partition = MessageUtil.jsonNodeToInt(_partitionNode, "PartitionData");
            }
            return _object;
        }
        public static JsonNode write(PartitionData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partition", new IntNode(_object.partition));
            return _node;
        }
        public static JsonNode write(PartitionData _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
