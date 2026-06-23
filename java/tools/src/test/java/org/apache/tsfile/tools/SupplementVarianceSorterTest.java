/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SupplementVarianceSorterTest {

  @Test
  public void testSortByPressureThenTemperature() {
    ImportSchema schema = buildSchema();
    SourceBatch batch =
        SourceBatch.fromRows(
            Arrays.asList("Region", "DeviceId", "Temperature", "Pressure"),
            Arrays.asList(
                new Object[] {"hebei", "1", "1", "100"},
                new Object[] {"hebei", "1", "3", "50"},
                new Object[] {"hebei", "1", "2", "200"}));

    SourceBatch sorted = SupplementVarianceSorter.sortByVariancePriority(batch, schema);

    assertEquals("50", sorted.getValue(0, 3).toString());
    assertEquals("3", sorted.getValue(0, 2).toString());
    assertEquals("100", sorted.getValue(1, 3).toString());
    assertEquals("1", sorted.getValue(1, 2).toString());
    assertEquals("200", sorted.getValue(2, 3).toString());
    assertEquals("2", sorted.getValue(2, 2).toString());
  }

  private static ImportSchema buildSchema() {
    ImportSchema schema = new ImportSchema();
    schema.setTableName("lab");
    schema.setTimeColumnName("Time");
    List<ImportSchema.TagColumn> tags = new ArrayList<>();
    tags.add(new ImportSchema.TagColumn("Region"));
    tags.add(new ImportSchema.TagColumn("DeviceId"));
    schema.setTagColumns(tags);
    schema.setSourceColumns(
        Arrays.asList(
            new ImportSchema.SourceColumn("Region", TSDataType.TEXT),
            new ImportSchema.SourceColumn("DeviceId", TSDataType.TEXT),
            new ImportSchema.SourceColumn("Time", TSDataType.INT64),
            new ImportSchema.SourceColumn("Temperature", TSDataType.FLOAT),
            new ImportSchema.SourceColumn("Pressure", TSDataType.DOUBLE)));
    return schema;
  }
}
