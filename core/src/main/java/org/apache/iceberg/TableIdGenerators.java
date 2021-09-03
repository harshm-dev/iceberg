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

import java.util.Objects;

/**
 * Contains the state of all id-generators that influence persisted data files of a table.
 *
 * <p>The contents of this object (and it's JSON representation) must be treated opaque, or:
 * internal to Iceberg.
 *
 * <p>For example Nessie uses the returned {@code TableIdGenerators} object to maintain differing
 * {@code TableMetadata}s across different references (branches and tags) but maintains the state
 * of the relevant ID generators across all references. This allows cherry-picking and merging
 * specific changes onto different branches.
 */
public final class TableIdGenerators {
  private final int formatVersion;
  private final int lastColumnId;
  private final int lastAssignedPartitionId;
  private final long lastSequenceNumber;

  private TableIdGenerators(int formatVersion, int lastColumnId, int lastAssignedPartitionId,
      long lastSequenceNumber) {
    this.formatVersion = formatVersion;
    this.lastColumnId = lastColumnId;
    this.lastAssignedPartitionId = lastAssignedPartitionId;
    this.lastSequenceNumber = lastSequenceNumber;
  }

  // intentionally package-private - use TableMetadata.getIdGenerators() or parse via TableIdGeneratorsParser
  static TableIdGenerators of(int formatVersion, int lastColumnId, int lastAssignedPartitionId,
      long lastSequenceNumber) {
    return new TableIdGenerators(formatVersion, lastColumnId, lastAssignedPartitionId, lastSequenceNumber);
  }

  public int formatVersion() {
    return formatVersion;
  }

  public int lastColumnId() {
    return lastColumnId;
  }

  public int lastAssignedPartitionId() {
    return lastAssignedPartitionId;
  }

  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  @Override
  public String toString() {
    return "TableIdGenerators{" +
        "formatVersion=" + formatVersion +
        ", lastColumnId=" + lastColumnId +
        ", lastAssignedPartitionId=" + lastAssignedPartitionId +
        ", lastSequenceNumber=" + lastSequenceNumber +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableIdGenerators that = (TableIdGenerators) o;
    return formatVersion == that.formatVersion &&
        lastColumnId == that.lastColumnId &&
        lastAssignedPartitionId == that.lastAssignedPartitionId &&
        lastSequenceNumber == that.lastSequenceNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(formatVersion, lastColumnId, lastAssignedPartitionId, lastSequenceNumber);
  }
}
