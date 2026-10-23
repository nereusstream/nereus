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
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.AddPartitionsToTxnResponseData.*;

public class AddPartitionsToTxnResponseDataJsonConverter {
    public static AddPartitionsToTxnResponseData read(JsonNode _node, short _version) {
        AddPartitionsToTxnResponseData _object = new AddPartitionsToTxnResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            throw new RuntimeException("AddPartitionsToTxnResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "AddPartitionsToTxnResponseData");
        }
        JsonNode _errorCodeNode = _node.get("errorCode");
        if (_errorCodeNode == null) {
            if (_version >= 4) {
                throw new RuntimeException("AddPartitionsToTxnResponseData: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = (short) 0;
            }
        } else {
            _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "AddPartitionsToTxnResponseData");
        }
        JsonNode _resultsByTransactionNode = _node.get("resultsByTransaction");
        if (_resultsByTransactionNode == null) {
            if (_version >= 4) {
                throw new RuntimeException("AddPartitionsToTxnResponseData: unable to locate field 'resultsByTransaction', which is mandatory in version " + _version);
            } else {
                _object.resultsByTransaction = new AddPartitionsToTxnResultCollection(0);
            }
        } else {
            if (!_resultsByTransactionNode.isArray()) {
                throw new RuntimeException("AddPartitionsToTxnResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            AddPartitionsToTxnResultCollection _collection = new AddPartitionsToTxnResultCollection(_resultsByTransactionNode.size());
            _object.resultsByTransaction = _collection;
            for (JsonNode _element : _resultsByTransactionNode) {
                _collection.add(AddPartitionsToTxnResultJsonConverter.read(_element, _version));
            }
        }
        JsonNode _resultsByTopicV3AndBelowNode = _node.get("resultsByTopicV3AndBelow");
        if (_resultsByTopicV3AndBelowNode == null) {
            if (_version <= 3) {
                throw new RuntimeException("AddPartitionsToTxnResponseData: unable to locate field 'resultsByTopicV3AndBelow', which is mandatory in version " + _version);
            } else {
                _object.resultsByTopicV3AndBelow = new AddPartitionsToTxnTopicResultCollection(0);
            }
        } else {
            if (!_resultsByTopicV3AndBelowNode.isArray()) {
                throw new RuntimeException("AddPartitionsToTxnResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            AddPartitionsToTxnTopicResultCollection _collection = new AddPartitionsToTxnTopicResultCollection(_resultsByTopicV3AndBelowNode.size());
            _object.resultsByTopicV3AndBelow = _collection;
            for (JsonNode _element : _resultsByTopicV3AndBelowNode) {
                _collection.add(AddPartitionsToTxnTopicResultJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(AddPartitionsToTxnResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        if (_version >= 4) {
            _node.set("errorCode", new ShortNode(_object.errorCode));
        }
        if (_version >= 4) {
            ArrayNode _resultsByTransactionArray = new ArrayNode(JsonNodeFactory.instance);
            for (AddPartitionsToTxnResult _element : _object.resultsByTransaction) {
                _resultsByTransactionArray.add(AddPartitionsToTxnResultJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("resultsByTransaction", _resultsByTransactionArray);
        } else {
            if (!_object.resultsByTransaction.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default resultsByTransaction at version " + _version);
            }
        }
        if (_version <= 3) {
            ArrayNode _resultsByTopicV3AndBelowArray = new ArrayNode(JsonNodeFactory.instance);
            for (AddPartitionsToTxnTopicResult _element : _object.resultsByTopicV3AndBelow) {
                _resultsByTopicV3AndBelowArray.add(AddPartitionsToTxnTopicResultJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("resultsByTopicV3AndBelow", _resultsByTopicV3AndBelowArray);
        } else {
            if (!_object.resultsByTopicV3AndBelow.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default resultsByTopicV3AndBelow at version " + _version);
            }
        }
        return _node;
    }
    public static JsonNode write(AddPartitionsToTxnResponseData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class AddPartitionsToTxnPartitionResultJsonConverter {
        public static AddPartitionsToTxnPartitionResult read(JsonNode _node, short _version) {
            AddPartitionsToTxnPartitionResult _object = new AddPartitionsToTxnPartitionResult();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("AddPartitionsToTxnPartitionResult: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "AddPartitionsToTxnPartitionResult");
            }
            JsonNode _partitionErrorCodeNode = _node.get("partitionErrorCode");
            if (_partitionErrorCodeNode == null) {
                throw new RuntimeException("AddPartitionsToTxnPartitionResult: unable to locate field 'partitionErrorCode', which is mandatory in version " + _version);
            } else {
                _object.partitionErrorCode = MessageUtil.jsonNodeToShort(_partitionErrorCodeNode, "AddPartitionsToTxnPartitionResult");
            }
            return _object;
        }
        public static JsonNode write(AddPartitionsToTxnPartitionResult _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            _node.set("partitionErrorCode", new ShortNode(_object.partitionErrorCode));
            return _node;
        }
        public static JsonNode write(AddPartitionsToTxnPartitionResult _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class AddPartitionsToTxnResultJsonConverter {
        public static AddPartitionsToTxnResult read(JsonNode _node, short _version) {
            AddPartitionsToTxnResult _object = new AddPartitionsToTxnResult();
            if (_version < 4) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of AddPartitionsToTxnResult");
            }
            JsonNode _transactionalIdNode = _node.get("transactionalId");
            if (_transactionalIdNode == null) {
                throw new RuntimeException("AddPartitionsToTxnResult: unable to locate field 'transactionalId', which is mandatory in version " + _version);
            } else {
                if (!_transactionalIdNode.isTextual()) {
                    throw new RuntimeException("AddPartitionsToTxnResult expected a string type, but got " + _node.getNodeType());
                }
                _object.transactionalId = _transactionalIdNode.asText();
            }
            JsonNode _topicResultsNode = _node.get("topicResults");
            if (_topicResultsNode == null) {
                throw new RuntimeException("AddPartitionsToTxnResult: unable to locate field 'topicResults', which is mandatory in version " + _version);
            } else {
                if (!_topicResultsNode.isArray()) {
                    throw new RuntimeException("AddPartitionsToTxnResult expected a JSON array, but got " + _node.getNodeType());
                }
                AddPartitionsToTxnTopicResultCollection _collection = new AddPartitionsToTxnTopicResultCollection(_topicResultsNode.size());
                _object.topicResults = _collection;
                for (JsonNode _element : _topicResultsNode) {
                    _collection.add(AddPartitionsToTxnTopicResultJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(AddPartitionsToTxnResult _object, short _version, boolean _serializeRecords) {
            if (_version < 4) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of AddPartitionsToTxnResult");
            }
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("transactionalId", new TextNode(_object.transactionalId));
            ArrayNode _topicResultsArray = new ArrayNode(JsonNodeFactory.instance);
            for (AddPartitionsToTxnTopicResult _element : _object.topicResults) {
                _topicResultsArray.add(AddPartitionsToTxnTopicResultJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("topicResults", _topicResultsArray);
            return _node;
        }
        public static JsonNode write(AddPartitionsToTxnResult _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class AddPartitionsToTxnTopicResultJsonConverter {
        public static AddPartitionsToTxnTopicResult read(JsonNode _node, short _version) {
            AddPartitionsToTxnTopicResult _object = new AddPartitionsToTxnTopicResult();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("AddPartitionsToTxnTopicResult: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("AddPartitionsToTxnTopicResult expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _resultsByPartitionNode = _node.get("resultsByPartition");
            if (_resultsByPartitionNode == null) {
                throw new RuntimeException("AddPartitionsToTxnTopicResult: unable to locate field 'resultsByPartition', which is mandatory in version " + _version);
            } else {
                if (!_resultsByPartitionNode.isArray()) {
                    throw new RuntimeException("AddPartitionsToTxnTopicResult expected a JSON array, but got " + _node.getNodeType());
                }
                AddPartitionsToTxnPartitionResultCollection _collection = new AddPartitionsToTxnPartitionResultCollection(_resultsByPartitionNode.size());
                _object.resultsByPartition = _collection;
                for (JsonNode _element : _resultsByPartitionNode) {
                    _collection.add(AddPartitionsToTxnPartitionResultJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(AddPartitionsToTxnTopicResult _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            ArrayNode _resultsByPartitionArray = new ArrayNode(JsonNodeFactory.instance);
            for (AddPartitionsToTxnPartitionResult _element : _object.resultsByPartition) {
                _resultsByPartitionArray.add(AddPartitionsToTxnPartitionResultJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("resultsByPartition", _resultsByPartitionArray);
            return _node;
        }
        public static JsonNode write(AddPartitionsToTxnTopicResult _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
