/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.v4;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TagFilterBuilder;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;

public class TagFilterExample {

  public static void main(String[] args) throws Exception {
    String path = "tag_filter_example.tsfile";
    File f = new File(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }

    // Define table schema
    TableSchema schema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("id1")
                    .dataType(TSDataType.STRING)
                    .category(ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("id2")
                    .dataType(TSDataType.STRING)
                    .category(ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.BOOLEAN)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.BOOLEAN)
                    .category(ColumnCategory.FIELD)
                    .build()));

    // Prepare tablet for writing data
    Tablet tablet =
        new Tablet(
            Arrays.asList("id1", "id2", "s1", "s2"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.BOOLEAN));

    // Row 0
    tablet.addTimestamp(0, 0);
    tablet.addValue(0, "id1", "id_01");
    tablet.addValue(0, "id2", "name_01");
    tablet.addValue(0, "s1", true);
    tablet.addValue(0, "s2", false);

    // Row 1
    tablet.addTimestamp(1, 1);
    tablet.addValue(1, "id1", "id_02");
    tablet.addValue(1, "id2", "name_02");
    tablet.addValue(1, "s1", false);
    tablet.addValue(1, "s2", true);

    // Row 2
    tablet.addTimestamp(2, 2);
    tablet.addValue(2, "id1", "id_03");
    tablet.addValue(2, "id2", "name_03");
    tablet.addValue(2, "s1", true);
    tablet.addValue(2, "s2", true);

    // Row 3
    tablet.addTimestamp(3, 3);
    tablet.addValue(3, "id1", "id_04");
    tablet.addValue(3, "id2", "name_04");
    tablet.addValue(3, "s1", false);
    tablet.addValue(3, "s2", false);

    // Write tablet to TsFile
    try (ITsFileWriter writer = new TsFileWriterBuilder().file(f).tableSchema(schema).build()) {
      writer.write(tablet);
    }

    // ---------------- TagFilter Examples ----------------
    TagFilterBuilder filterBuilder = new TagFilterBuilder(schema);

    // eq
    queryWithFilter(f, schema, filterBuilder.eq("id1", "id_02"), "eq(id1 = id_02)");

    // neq
    queryWithFilter(f, schema, filterBuilder.neq("id1", "id_02"), "neq(id1 != id_02)");

    // lt
    queryWithFilter(f, schema, filterBuilder.lt("id1", "id_03"), "lt(id1 < id_03)");

    // lteq
    queryWithFilter(f, schema, filterBuilder.lteq("id1", "id_03"), "lteq(id1 <= id_03)");

    // gt
    queryWithFilter(f, schema, filterBuilder.gt("id1", "id_02"), "gt(id1 > id_02)");

    // gteq
    queryWithFilter(f, schema, filterBuilder.gteq("id1", "id_02"), "gteq(id1 >= id_02)");

    // betweenAnd
    queryWithFilter(
        f,
        schema,
        filterBuilder.betweenAnd("id1", "id_02", "id_03"),
        "betweenAnd(id1 between id_02 and id_03)");

    // notBetweenAnd
    queryWithFilter(
        f,
        schema,
        filterBuilder.notBetweenAnd("id1", "id_02", "id_03"),
        "notBetweenAnd(id1 not between id_02 and id_03)");

    // and
    queryWithFilter(
        f,
        schema,
        filterBuilder.and(filterBuilder.gteq("id1", "id_02"), filterBuilder.lteq("id1", "id_03")),
        "and(gteq(id1,id_02), lteq(id1,id_03))");

    // or
    queryWithFilter(
        f,
        schema,
        filterBuilder.or(filterBuilder.eq("id1", "id_01"), filterBuilder.eq("id1", "id_04")),
        "or(eq(id1,id_01), eq(id1,id_04))");

    // not
    queryWithFilter(
        f, schema, filterBuilder.not(filterBuilder.eq("id1", "id_02")), "not(eq(id1,id_02))");

    // regExp
    queryWithFilter(
        f, schema, filterBuilder.regExp("id1", "id_0[23]"), "regExp(id1 matches id_0[23])");

    // notRegExp
    queryWithFilter(
        f,
        schema,
        filterBuilder.notRegExp("id1", "id_0[23]"),
        "notRegExp(id1 not matches id_0[23])");

    // like
    queryWithFilter(f, schema, filterBuilder.like("id1", "id_0_"), "like(id1 like id_0_)");

    // notLike
    queryWithFilter(
        f, schema, filterBuilder.notLike("id1", "id_0_"), "notLike(id1 not like id_0_)");
    new File(path).delete();
  }

  /** Helper method to execute a query with a given filter and print results. */
  private static void queryWithFilter(File f, TableSchema schema, Object filter, String desc)
      throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs =
            reader.query("t1", Arrays.asList("id1", "id2", "s1", "s2"), 0, 3, (Filter) filter)) {

      System.out.println("=== Query with TagFilter " + desc + " ===");
      while (rs.next()) {
        long time = rs.getLong("Time");
        String id1 = rs.getString("id1");
        String id2 = rs.getString("id2");
        Boolean s1 = rs.isNull("s1") ? null : rs.getBoolean("s1");
        Boolean s2 = rs.isNull("s2") ? null : rs.getBoolean("s2");

        System.out.printf("time=%d, id1=%s, id2=%s, s1=%s, s2=%s%n", time, id1, id2, s1, s2);
      }
    }
  }
}
