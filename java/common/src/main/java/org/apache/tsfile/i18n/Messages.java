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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * Resolves log and exception message patterns from locale-specific resource bundles.
 *
 * <p>Locale resolution order (evaluated once at class load):
 *
 * <ol>
 *   <li>System property {@code -Dtsfile.locale} (e.g. {@code zh}, {@code zh-CN}).
 *   <li>{@link Locale#getDefault()}.
 *   <li>English fallback via {@link ResourceBundle}'s root-bundle mechanism.
 * </ol>
 *
 * <p>Use {@link #get(String)} for SLF4J patterns (which contain {@code &#123;&#125;} placeholders
 * and are formatted lazily by SLF4J), and {@link #format(String, Object...)} for exception messages
 * (which use {@link String#format} positional placeholders like {@code %1$s}).
 */
public final class Messages {

  private static final String BUNDLE_BASE_NAME = "org.apache.tsfile.i18n.messages";
  private static final ResourceBundle BUNDLE = loadBundle();

  private Messages() {}

  /**
   * Returns the raw message pattern.
   *
   * @throws java.util.MissingResourceException if {@code key} is not defined in the bundle
   */
  public static String get(String key) {
    return BUNDLE.getString(key);
  }

  /**
   * Returns the message pattern formatted with the given arguments via {@link String#format}.
   *
   * @throws java.util.MissingResourceException if {@code key} is not defined in the bundle
   * @throws java.util.IllegalFormatException if the pattern and {@code args} are incompatible
   */
  public static String format(String key, Object... args) {
    return String.format(BUNDLE.getString(key), args);
  }

  private static ResourceBundle loadBundle() {
    return ResourceBundle.getBundle(
        BUNDLE_BASE_NAME, determineLocale(), Messages.class.getClassLoader(), new Utf8Control());
  }

  private static Locale determineLocale() {
    String prop = System.getProperty("tsfile.locale");
    if (prop != null && !prop.isEmpty()) {
      return Locale.forLanguageTag(prop);
    }
    return Locale.getDefault();
  }

  /**
   * Loads {@code .properties} files as UTF-8 instead of the JDK default ISO-8859-1. Lets us write
   * Chinese characters directly in {@code messages_zh.properties} without Unicode escapes.
   */
  private static final class Utf8Control extends ResourceBundle.Control {
    /**
     * Disable Java's default fallback to {@link Locale#getDefault()}. Without this override, when a
     * user requests "en" (via {@code -Dtsfile.locale=en}) on a JVM whose default locale is "zh",
     * ResourceBundle's default behavior is to ALSO load the zh bundle as a parent, which causes
     * {@code BUNDLE.getString(key)} to return Chinese text. Returning {@code null} keeps the
     * resolution strictly within the requested locale's candidate chain (e.g., {@code [en, ROOT]}).
     */
    @Override
    public Locale getFallbackLocale(String baseName, Locale locale) {
      return null;
    }

    @Override
    public ResourceBundle newBundle(
        String baseName, Locale locale, String format, ClassLoader loader, boolean reload)
        throws IOException, IllegalAccessException, InstantiationException {
      if (!"java.properties".equals(format)) {
        return super.newBundle(baseName, locale, format, loader, reload);
      }
      String bundleName = toBundleName(baseName, locale);
      String resourceName = toResourceName(bundleName, "properties");
      try (InputStream in = loader.getResourceAsStream(resourceName)) {
        if (in == null) {
          return null;
        }
        try (Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
          return new PropertyResourceBundle(reader);
        }
      }
    }
  }
}
