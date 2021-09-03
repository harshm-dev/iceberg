/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public final class TableIdGeneratorsParser {

  private TableIdGeneratorsParser() {
  }

  // visible for testing
  static final String FORMAT_VERSION = TableMetadataParser.FORMAT_VERSION;
  static final String LAST_SEQUENCE_NUMBER = TableMetadataParser.LAST_SEQUENCE_NUMBER;
  static final String LAST_COLUMN_ID = TableMetadataParser.LAST_COLUMN_ID;
  static final String LAST_PARTITION_ID = TableMetadataParser.LAST_PARTITION_ID;

  public static String toJson(TableIdGenerators idGenerators) {
    try (StringWriter writer = new StringWriter()) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      idGeneratorsJson(idGenerators, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", idGenerators);
    }
  }

  private static void idGeneratorsJson(TableIdGenerators idGenerators, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(FORMAT_VERSION, idGenerators.formatVersion());
    generator.writeNumberField(LAST_COLUMN_ID, idGenerators.lastColumnId());
    generator.writeNumberField(LAST_PARTITION_ID, idGenerators.lastAssignedPartitionId());
    generator.writeNumberField(LAST_SEQUENCE_NUMBER, idGenerators.lastSequenceNumber());
    generator.writeEndObject();
  }

  public static TableIdGenerators parseJson(String json) {
    try {
      return idGeneratorFromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to parse TableIdGenerator JSON");
    }
  }

  private static TableIdGenerators idGeneratorFromJson(JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse metadata from a non-object: %s", node);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(formatVersion <= TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION,
        "Cannot read unsupported version %s", formatVersion);

    int lastAssignedColumnId = JsonUtil.getInt(LAST_COLUMN_ID, node);
    Integer lastAssignedPartitionId = JsonUtil.getIntOrNull(LAST_PARTITION_ID, node);
    long lastSequenceNumber = JsonUtil.getLong(LAST_SEQUENCE_NUMBER, node);

    return TableIdGenerators.of(formatVersion, lastAssignedColumnId,
        lastAssignedPartitionId != null ? lastAssignedPartitionId : 0, lastSequenceNumber);
  }
}
