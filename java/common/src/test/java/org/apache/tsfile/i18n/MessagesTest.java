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

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

  private static final String BUNDLE = "org.apache.tsfile.i18n.messages";

  @Test
  public void enAndZhKeysMatch() {
    Set<String> en = keysOf(loadBundle(Locale.ROOT));
    Set<String> zh = keysOf(loadBundle(Locale.SIMPLIFIED_CHINESE));
    Set<String> missingInZh = new HashSet<>(en);
    missingInZh.removeAll(zh);
    Set<String> extraInZh = new HashSet<>(zh);
    extraInZh.removeAll(en);
    assertTrue("keys present in en but missing in zh: " + missingInZh, missingInZh.isEmpty());
    assertTrue("keys present in zh but not in en: " + extraInZh, extraInZh.isEmpty());
  }

  @Test
  public void allEnKeysResolveNonEmpty() {
    assertAllValuesNonEmpty(loadBundle(Locale.ROOT));
  }

  @Test
  public void allZhKeysResolveNonEmpty() {
    assertAllValuesNonEmpty(loadBundle(Locale.SIMPLIFIED_CHINESE));
  }

  private static void assertAllValuesNonEmpty(ResourceBundle bundle) {
    for (String key : Collections.list(bundle.getKeys())) {
      String value = bundle.getString(key);
      assertNotNull("null value for key " + key, value);
      assertFalse("empty value for key " + key, value.trim().isEmpty());
    }
  }

  private static ResourceBundle loadBundle(Locale locale) {
    return ResourceBundle.getBundle(
        BUNDLE, locale, MessagesTest.class.getClassLoader(), new Utf8TestControl());
  }

  private static Set<String> keysOf(ResourceBundle bundle) {
    return new HashSet<>(Collections.list(bundle.getKeys()));
  }

  private static final class Utf8TestControl extends ResourceBundle.Control {
    @Override
    public java.util.ResourceBundle newBundle(
        String baseName, Locale locale, String format, ClassLoader loader, boolean reload)
        throws java.io.IOException, IllegalAccessException, InstantiationException {
      if (!"java.properties".equals(format)) {
        return super.newBundle(baseName, locale, format, loader, reload);
      }
      String bundleName = toBundleName(baseName, locale);
      String resourceName = toResourceName(bundleName, "properties");
      try (java.io.InputStream in = loader.getResourceAsStream(resourceName)) {
        if (in == null) {
          return null;
        }
        try (java.io.Reader reader =
            new java.io.InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8)) {
          return new java.util.PropertyResourceBundle(reader);
        }
      }
    }
  }
}
