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

import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsFileToolFormatFilterTest {

  @After
  public void resetFormatStr() throws Exception {
    setFormatStr(null);
  }

  // --- No --format set: only known extensions are accepted ---

  @Test
  public void testNoFormatAcceptsCsv() throws Exception {
    setFormatStr(null);
    assertTrue(callIsAcceptedFormat("data.csv"));
  }

  @Test
  public void testNoFormatAcceptsParquet() throws Exception {
    setFormatStr(null);
    assertTrue(callIsAcceptedFormat("data.parquet"));
  }

  @Test
  public void testNoFormatAcceptsArrowVariants() throws Exception {
    setFormatStr(null);
    assertTrue(callIsAcceptedFormat("data.arrow"));
    assertTrue(callIsAcceptedFormat("data.ipc"));
    assertTrue(callIsAcceptedFormat("data.feather"));
  }

  @Test
  public void testNoFormatRejectsReadmeAndLog() throws Exception {
    setFormatStr(null);
    assertFalse(callIsAcceptedFormat("README.md"));
    assertFalse(callIsAcceptedFormat("import.log"));
    assertFalse(callIsAcceptedFormat("notes.txt"));
  }

  // --- --format=csv: only .csv passes; README/.log/.parquet do NOT ---

  @Test
  public void testCsvFormatRejectsForeignExtensions() throws Exception {
    setFormatStr("csv");
    assertTrue(callIsAcceptedFormat("data.csv"));
    assertFalse(callIsAcceptedFormat("README.md"));
    assertFalse(callIsAcceptedFormat("import.log"));
    assertFalse(callIsAcceptedFormat("data.parquet"));
    assertFalse(callIsAcceptedFormat("data.arrow"));
    assertFalse(callIsAcceptedFormat("noext"));
  }

  // --- --format=parquet: only .parquet passes ---

  @Test
  public void testParquetFormatRejectsForeignExtensions() throws Exception {
    setFormatStr("parquet");
    assertTrue(callIsAcceptedFormat("data.parquet"));
    assertFalse(callIsAcceptedFormat("data.csv"));
    assertFalse(callIsAcceptedFormat("README.md"));
  }

  // --- --format=arrow: .arrow/.ipc/.feather pass, others rejected ---

  @Test
  public void testArrowFormatRejectsForeignExtensions() throws Exception {
    setFormatStr("arrow");
    assertTrue(callIsAcceptedFormat("data.arrow"));
    assertTrue(callIsAcceptedFormat("data.ipc"));
    assertTrue(callIsAcceptedFormat("data.feather"));
    assertFalse(callIsAcceptedFormat("data.csv"));
    assertFalse(callIsAcceptedFormat("README.md"));
  }

  // --- Case-insensitive extension matching ---

  @Test
  public void testUpperCaseExtensionAccepted() throws Exception {
    setFormatStr("csv");
    assertTrue(callIsAcceptedFormat("DATA.CSV"));
  }

  // --- resolveFormat still works for files that already pass the filter ---

  @Test
  public void testResolveFormatByExtension() throws Exception {
    setFormatStr(null);
    assertEquals("csv", callResolveFormat("data.csv"));
    assertEquals("parquet", callResolveFormat("data.parquet"));
    assertEquals("arrow", callResolveFormat("data.feather"));
  }

  // --- helpers ---

  private static void setFormatStr(String value) throws Exception {
    Field f = TsFileTool.class.getDeclaredField("formatStr");
    f.setAccessible(true);
    f.set(null, value);
  }

  private static boolean callIsAcceptedFormat(String fileName) throws Exception {
    Method m = TsFileTool.class.getDeclaredMethod("isAcceptedFormat", String.class);
    m.setAccessible(true);
    return (boolean) m.invoke(null, fileName);
  }

  private static String callResolveFormat(String fileName) throws Exception {
    Method m = TsFileTool.class.getDeclaredMethod("resolveFormat", String.class);
    m.setAccessible(true);
    return (String) m.invoke(null, fileName);
  }
}
