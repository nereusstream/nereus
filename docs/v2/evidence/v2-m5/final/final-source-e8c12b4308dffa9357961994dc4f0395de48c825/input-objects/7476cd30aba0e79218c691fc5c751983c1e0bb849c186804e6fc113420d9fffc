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

package org.apache.kafka.common.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import static org.apache.kafka.common.metadata.BeginTransactionRecord.*;

public class BeginTransactionRecordJsonConverter {
    public static BeginTransactionRecord read(JsonNode _node, short _version) {
        BeginTransactionRecord _object = new BeginTransactionRecord();
        JsonNode _nameNode = _node.get("name");
        if (_nameNode == null) {
            _object.name = null;
        } else {
            if (_nameNode.isNull()) {
                _object.name = null;
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("BeginTransactionRecord expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
        }
        return _object;
    }
    public static JsonNode write(BeginTransactionRecord _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_object.name != null) {
            _node.set("name", new TextNode(_object.name));
        }
        return _node;
    }
    public static JsonNode write(BeginTransactionRecord _object, short _version) {
        return write(_object, _version, true);
    }
}
