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

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AutoSchemaInfererTest {

  // ===== Time column detection =====

  @Test
  public void testDetectTimeColumnLowercase() {
    assertEquals("time", AutoSchemaInferer.detectTimeColumn(Arrays.asList("time", "value")));
  }

  @Test
  public void testDetectTimeColumnUppercase() {
    assertEquals("TIME", AutoSchemaInferer.detectTimeColumn(Arrays.asList("TIME", "value")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDetectTimeColumnMixedCaseFails() {
    AutoSchemaInferer.detectTimeColumn(Arrays.asList("Time", "value"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDetectTimeColumnRandomCaseFails() {
    AutoSchemaInferer.detectTimeColumn(Arrays.asList("tIME", "value"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDetectTimeColumnBothTimeAndTIMEFails() {
    AutoSchemaInferer.detectTimeColumn(Arrays.asList("time", "TIME", "value"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDetectTimeColumnDuplicateTimeFails() {
    AutoSchemaInferer.detectTimeColumn(Arrays.asList("time", "time", "value"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDetectTimeColumnNoMatchFails() {
    AutoSchemaInferer.detectTimeColumn(Arrays.asList("ts", "value"));
  }

  // ===== Type inference =====

  @Test
  public void testInferAllIntegers() {
    List<String> cols = Arrays.asList("time", "count");
    List<Object[]> rows =
        Arrays.asList(
            new Object[] {"1000", "42"}, new Object[] {"2000", "99"}, new Object[] {"3000", "-7"});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.INT64, types[0]);
    assertEquals(TSDataType.INT64, types[1]);
  }

  @Test
  public void testInferIntegerThenDecimalPromotesToDouble() {
    List<String> cols = Arrays.asList("time", "value");
    List<Object[]> rows = Arrays.asList(new Object[] {"1000", "42"}, new Object[] {"2000", "3.14"});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.DOUBLE, types[1]);
  }

  @Test
  public void testInferNumericThenNonNumericPromotesToString() {
    List<String> cols = Arrays.asList("time", "mixed");
    List<Object[]> rows =
        Arrays.asList(new Object[] {"1000", "42"}, new Object[] {"2000", "hello"});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.STRING, types[1]);
  }

  @Test
  public void testInferBooleanThenNonBooleanPromotesToString() {
    List<String> cols = Arrays.asList("time", "flag");
    List<Object[]> rows = Arrays.asList(new Object[] {"1000", "true"}, new Object[] {"2000", "42"});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.STRING, types[1]);
  }

  @Test
  public void testInferAllBoolean() {
    List<String> cols = Arrays.asList("time", "flag");
    List<Object[]> rows =
        Arrays.asList(
            new Object[] {"1000", "true"},
            new Object[] {"2000", "false"},
            new Object[] {"3000", "True"},
            new Object[] {"4000", "FALSE"});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.BOOLEAN, types[1]);
  }

  @Test
  public void testInferEmptyStringSkipped() {
    List<String> cols = Arrays.asList("time", "value");
    List<Object[]> rows = Arrays.asList(new Object[] {"1000", ""}, new Object[] {"2000", "42"});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.INT64, types[1]);
  }

  @Test
  public void testInferBackslashNSkipped() {
    List<String> cols = Arrays.asList("time", "value");
    List<Object[]> rows =
        Arrays.asList(new Object[] {"1000", "\\N"}, new Object[] {"2000", "3.14"});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.DOUBLE, types[1]);
  }

  @Test
  public void testInferAllUnknownDefaultsToString() {
    List<String> cols = Arrays.asList("time", "empty_col");
    List<Object[]> rows = Arrays.asList(new Object[] {"1000", ""}, new Object[] {"2000", null});

    TSDataType[] types =
        AutoSchemaInferer.inferColumnTypes(
            cols, rows, "time", AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);
    assertEquals(TSDataType.STRING, types[1]);
  }

  // ===== Null token recognition =====

  @Test
  public void testEmptyCellIsNullToken() {
    assertTrue(AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS.contains(""));
  }

  @Test
  public void testBackslashNIsNullToken() {
    assertTrue(AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS.contains("\\N"));
  }

  @Test
  public void testUppercaseNULLNotNullToken() {
    assertTrue(!AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS.contains("NULL"));
  }

  @Test
  public void testLowercaseNullNotNullToken() {
    assertTrue(!AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS.contains("null"));
  }

  @Test
  public void testNaNNotNullToken() {
    assertTrue(!AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS.contains("NaN"));
  }

  // ===== Default table name =====

  @Test
  public void testDeriveTableNameFromCsvFile() {
    assertEquals("sensor_data", AutoSchemaInferer.deriveTableName("sensor_data.csv", "csv_data"));
  }

  @Test
  public void testDeriveTableNameFromParquetFile() {
    assertEquals("motor", AutoSchemaInferer.deriveTableName("motor.parquet", "parquet_data"));
  }

  @Test
  public void testDeriveTableNameSpecialChars() {
    assertEquals("my_data_2025", AutoSchemaInferer.deriveTableName("my-data@2025.csv", "csv_data"));
  }

  @Test
  public void testDeriveTableNameEmptyAfterClean() {
    assertEquals("csv_data", AutoSchemaInferer.deriveTableName("@#$.csv", "csv_data"));
  }

  @Test
  public void testDeriveTableNameDigitPrefix() {
    assertEquals("t_123abc", AutoSchemaInferer.deriveTableName("123abc.csv", "csv_data"));
  }

  @Test
  public void testDeriveTableNameDigitOnly() {
    assertEquals("t_123", AutoSchemaInferer.deriveTableName("123.csv", "csv_data"));
  }

  @Test
  public void testDeriveTableNameNoExtension() {
    assertEquals("datafile", AutoSchemaInferer.deriveTableName("datafile", "csv_data"));
  }

  @Test
  public void testDeriveTableNameUnknownExtension() {
    assertEquals("data", AutoSchemaInferer.deriveTableName("data.txt", "csv_data"));
  }

  // ===== buildAutoSchema =====

  @Test
  public void testBuildAutoSchemaBasic() {
    List<String> cols = Arrays.asList("time", "temp", "humidity");
    TSDataType[] types = {TSDataType.INT64, TSDataType.DOUBLE, TSDataType.DOUBLE};

    ImportSchema schema = AutoSchemaInferer.buildAutoSchema("test", "time", cols, types, "ms");

    assertEquals("test", schema.getTableName());
    assertEquals("time", schema.getTimeColumnName());
    assertEquals("ms", schema.getTimePrecision());
    assertTrue(schema.getTagColumns().isEmpty());
    assertEquals(3, schema.getSourceColumns().size());

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(2, fields.size());
    assertEquals("temp", fields.get(0).getName());
    assertEquals(TSDataType.DOUBLE, fields.get(0).getDataType());
    assertEquals("humidity", fields.get(1).getName());
  }

  @Test
  public void testBuildAutoSchemaDefaultPrecision() {
    List<String> cols = Arrays.asList("time", "val");
    TSDataType[] types = {TSDataType.INT64, TSDataType.STRING};

    ImportSchema schema = AutoSchemaInferer.buildAutoSchema("t", "time", cols, types, null);
    assertEquals("ms", schema.getTimePrecision());
  }

  // ===== Cell classification =====

  @Test
  public void testClassifyCellBoolean() {
    assertEquals(AutoSchemaInferer.InferredType.BOOLEAN, AutoSchemaInferer.classifyCell("true"));
    assertEquals(AutoSchemaInferer.InferredType.BOOLEAN, AutoSchemaInferer.classifyCell("false"));
    assertEquals(AutoSchemaInferer.InferredType.BOOLEAN, AutoSchemaInferer.classifyCell("True"));
    assertEquals(AutoSchemaInferer.InferredType.BOOLEAN, AutoSchemaInferer.classifyCell("FALSE"));
  }

  @Test
  public void testClassifyCellInteger() {
    assertEquals(AutoSchemaInferer.InferredType.INT64, AutoSchemaInferer.classifyCell("42"));
    assertEquals(AutoSchemaInferer.InferredType.INT64, AutoSchemaInferer.classifyCell("-7"));
    assertEquals(AutoSchemaInferer.InferredType.INT64, AutoSchemaInferer.classifyCell("0"));
  }

  @Test
  public void testClassifyCellDouble() {
    assertEquals(AutoSchemaInferer.InferredType.DOUBLE, AutoSchemaInferer.classifyCell("3.14"));
    assertEquals(AutoSchemaInferer.InferredType.DOUBLE, AutoSchemaInferer.classifyCell("-1.5"));
    assertEquals(AutoSchemaInferer.InferredType.DOUBLE, AutoSchemaInferer.classifyCell(".5"));
  }

  @Test
  public void testClassifyCellString() {
    assertEquals(AutoSchemaInferer.InferredType.STRING, AutoSchemaInferer.classifyCell("hello"));
    assertEquals(AutoSchemaInferer.InferredType.STRING, AutoSchemaInferer.classifyCell("abc123"));
  }

  // ===== Type promotion =====

  @Test
  public void testPromoteUnknownTakesIncoming() {
    assertEquals(
        AutoSchemaInferer.InferredType.INT64,
        AutoSchemaInferer.promote(
            AutoSchemaInferer.InferredType.UNKNOWN, AutoSchemaInferer.InferredType.INT64));
  }

  @Test
  public void testPromoteSameTypeStays() {
    assertEquals(
        AutoSchemaInferer.InferredType.INT64,
        AutoSchemaInferer.promote(
            AutoSchemaInferer.InferredType.INT64, AutoSchemaInferer.InferredType.INT64));
  }

  @Test
  public void testPromoteInt64DoubleBecomesDouble() {
    assertEquals(
        AutoSchemaInferer.InferredType.DOUBLE,
        AutoSchemaInferer.promote(
            AutoSchemaInferer.InferredType.INT64, AutoSchemaInferer.InferredType.DOUBLE));
  }

  @Test
  public void testPromoteBooleanInt64BecomesString() {
    assertEquals(
        AutoSchemaInferer.InferredType.STRING,
        AutoSchemaInferer.promote(
            AutoSchemaInferer.InferredType.BOOLEAN, AutoSchemaInferer.InferredType.INT64));
  }

  @Test
  public void testPromoteDoubleStringBecomesString() {
    assertEquals(
        AutoSchemaInferer.InferredType.STRING,
        AutoSchemaInferer.promote(
            AutoSchemaInferer.InferredType.DOUBLE, AutoSchemaInferer.InferredType.STRING));
  }
}
