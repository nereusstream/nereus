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
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.DescribeTopicPartitionsRequestData.*;

public class DescribeTopicPartitionsRequestDataJsonConverter {
    public static DescribeTopicPartitionsRequestData read(JsonNode _node, short _version) {
        DescribeTopicPartitionsRequestData _object = new DescribeTopicPartitionsRequestData();
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("DescribeTopicPartitionsRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("DescribeTopicPartitionsRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<TopicRequest> _collection = new ArrayList<TopicRequest>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(TopicRequestJsonConverter.read(_element, _version));
            }
        }
        JsonNode _responsePartitionLimitNode = _node.get("responsePartitionLimit");
        if (_responsePartitionLimitNode == null) {
            throw new RuntimeException("DescribeTopicPartitionsRequestData: unable to locate field 'responsePartitionLimit', which is mandatory in version " + _version);
        } else {
            _object.responsePartitionLimit = MessageUtil.jsonNodeToInt(_responsePartitionLimitNode, "DescribeTopicPartitionsRequestData");
        }
        JsonNode _cursorNode = _node.get("cursor");
        if (_cursorNode == null) {
            throw new RuntimeException("DescribeTopicPartitionsRequestData: unable to locate field 'cursor', which is mandatory in version " + _version);
        } else {
            if (_cursorNode.isNull()) {
                _object.cursor = null;
            } else {
                _object.cursor = CursorJsonConverter.read(_cursorNode, _version);
            }
        }
        return _object;
    }
    public static JsonNode write(DescribeTopicPartitionsRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (TopicRequest _element : _object.topics) {
            _topicsArray.add(TopicRequestJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        _node.set("responsePartitionLimit", new IntNode(_object.responsePartitionLimit));
        if (_object.cursor == null) {
            _node.set("cursor", NullNode.instance);
        } else {
            _node.set("cursor", CursorJsonConverter.write(_object.cursor, _version, _serializeRecords));
        }
        return _node;
    }
    public static JsonNode write(DescribeTopicPartitionsRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class CursorJsonConverter {
        public static Cursor read(JsonNode _node, short _version) {
            Cursor _object = new Cursor();
            JsonNode _topicNameNode = _node.get("topicName");
            if (_topicNameNode == null) {
                throw new RuntimeException("Cursor: unable to locate field 'topicName', which is mandatory in version " + _version);
            } else {
                if (!_topicNameNode.isTextual()) {
                    throw new RuntimeException("Cursor expected a string type, but got " + _node.getNodeType());
                }
                _object.topicName = _topicNameNode.asText();
            }
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("Cursor: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "Cursor");
            }
            return _object;
        }
        public static JsonNode write(Cursor _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("topicName", new TextNode(_object.topicName));
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            return _node;
        }
        public static JsonNode write(Cursor _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TopicRequestJsonConverter {
        public static TopicRequest read(JsonNode _node, short _version) {
            TopicRequest _object = new TopicRequest();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("TopicRequest: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("TopicRequest expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            return _object;
        }
        public static JsonNode write(TopicRequest _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            return _node;
        }
        public static JsonNode write(TopicRequest _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
