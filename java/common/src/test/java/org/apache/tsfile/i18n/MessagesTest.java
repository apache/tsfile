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
package org.apache.tsfile.i18n;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessagesTest {

  static {
    // Pin locale so tests run identically regardless of JVM default. Must be set
    // before any reference to Messages, since Messages.BUNDLE is initialized at
    // class load. JUnit loads test classes (and runs their static blocks) before
    // invoking test methods, so this static block reliably fires first.
    System.setProperty("tsfile.locale", "en");
  }

  @Test
  public void getReturnsRawPattern() {
    assertEquals("hello %1$s", Messages.get("test.seed"));
  }

  @Test
  public void formatSubstitutesArgs() {
    assertEquals("hello world", Messages.format("test.seed", "world"));
  }
}
