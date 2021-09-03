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

import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIdGeneratorsSerialization extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestIdGeneratorsSerialization(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testSerialization() {
    TableMetadata meta = table.ops().current();

    TableIdGenerators gen = meta.getIdGenerators();
    verifyReserialization(gen);

    ThreadLocalRandom tlr = ThreadLocalRandom.current();
    for (int i = 0; i < 10; i++) {
      gen = TableIdGenerators.of(formatVersion, tlr.nextInt(), tlr.nextInt(), tlr.nextLong());
      verifyReserialization(gen);
    }
  }

  private void verifyReserialization(TableIdGenerators gen) {
    String json = TableIdGeneratorsParser.toJson(gen);
    TableIdGenerators result = TableIdGeneratorsParser.parseJson(json);

    Assert.assertEquals(result, gen);
    Assert.assertEquals(result.formatVersion(), gen.formatVersion());
    Assert.assertEquals(result.lastColumnId(), gen.lastColumnId());
    Assert.assertEquals(result.lastAssignedPartitionId(), gen.lastAssignedPartitionId());
    Assert.assertEquals(result.lastSequenceNumber(), gen.lastSequenceNumber());
  }
}
