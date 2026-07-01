/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.external.commons.lang3;

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

import org.apache.tsfile.external.commons.lang3.function.Suppliers;

import java.util.function.Supplier;

public class SystemProperties {

  /** The System property name {@value}. */
  public static final String OS_NAME = "os.name";

  /** The System property name {@value}. */
  public static final String JAVA_SPECIFICATION_VERSION = "java.specification.version";

  /**
   * Gets the current value from the system properties map for {@value #OS_NAME}.
   *
   * <p>Returns {@code null} if the property cannot be read due to a {@link SecurityException}.
   *
   * @return the current value from the system properties map.
   */
  public static String getOsName() {
    return getProperty(OS_NAME);
  }

  /**
   * Gets the current value from the system properties map.
   *
   * <p>Returns {@code null} if the property cannot be read due to a {@link SecurityException}.
   *
   * @return the current value from the system properties map.
   */
  public static String getJavaSpecificationVersion() {
    return getProperty(JAVA_SPECIFICATION_VERSION);
  }

  /**
   * Gets a System property, defaulting to {@code null} if the property cannot be read.
   *
   * <p>If a {@link SecurityException} is caught, the return value is {@code null}.
   *
   * @param property the system property name
   * @return the system property value or {@code null} if a security problem occurs
   */
  public static String getProperty(final String property) {
    return getProperty(property, Suppliers.nul());
  }

  /**
   * Gets a System property, defaulting to {@code null} if the property cannot be read.
   *
   * <p>If a {@link SecurityException} is caught, the return value is {@code null}.
   *
   * @param property the system property name.
   * @param defaultIfAbsent get this Supplier when the property is empty or throws
   *     SecurityException.
   * @return the system property value or {@code null} if a security problem occurs
   */
  static String getProperty(final String property, final Supplier<String> defaultIfAbsent) {
    try {
      if (StringUtils.isEmpty(property)) {
        return Suppliers.get(defaultIfAbsent);
      }
      return StringUtils.getIfEmpty(System.getProperty(property), defaultIfAbsent);
    } catch (final SecurityException ignore) {
      // We are not allowed to look at this property.
      //
      // System.err.println("Caught a SecurityException reading the system property '" + property
      // + "'; the SystemUtils property value will default to null.");
      return defaultIfAbsent.get();
    }
  }
}
