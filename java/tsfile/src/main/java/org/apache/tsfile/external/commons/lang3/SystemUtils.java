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

public class SystemUtils {

  /** The prefix String for all Windows OS. */
  private static final String OS_NAME_WINDOWS_PREFIX = "Windows";

  /**
   * A constant for the System Property {@code java.specification.version}. Java Runtime Environment
   * specification version.
   *
   * <p>Defaults to {@code null} if the runtime does not have security access to read this property
   * or the property does not exist.
   *
   * <p>This value is initialized when the class is loaded. If {@link
   * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
   * called after this class is loaded, the value will be out of sync with that System property.
   *
   * @see SystemProperties#getJavaSpecificationVersion()
   * @since Java 1.3
   */
  public static final String JAVA_SPECIFICATION_VERSION =
      SystemProperties.getJavaSpecificationVersion();

  /**
   * A constant for the System Property {@code os.name}. Operating system name.
   *
   * <p>Defaults to {@code null} if the runtime does not have security access to read this property
   * or the property does not exist.
   *
   * <p>This value is initialized when the class is loaded. If {@link
   * System#setProperty(String,String)} or {@link System#setProperties(java.util.Properties)} is
   * called after this class is loaded, the value will be out of sync with that System property.
   *
   * @see SystemProperties#getOsName()
   * @since Java 1.1
   */
  public static final String OS_NAME = SystemProperties.getOsName();

  /**
   * The constant {@code true} if this is Windows.
   *
   * <p>The result depends on the value of the {@link #OS_NAME} constant.
   *
   * <p>The field will return {@code false} if {@link #OS_NAME} is {@code null}.
   *
   * <p>This value is initialized when the class is loaded.
   *
   * @since 2.0
   */
  public static final boolean IS_OS_WINDOWS = getOsNameMatches(OS_NAME_WINDOWS_PREFIX);

  /**
   * The constant {@code true} if this is Java version 1.8 (also 1.8.x versions).
   *
   * <p>The result depends on the value of the {@link #JAVA_SPECIFICATION_VERSION} constant.
   *
   * <p>The field will return {@code false} if {@link #JAVA_SPECIFICATION_VERSION} is {@code null}.
   *
   * <p>This value is initialized when the class is loaded.
   *
   * @since 3.3.2
   */
  public static final boolean IS_JAVA_1_8 = getJavaVersionMatches("1.8");

  /**
   * Tests if the operating system matches the given string with a case-insensitive comparison.
   *
   * <p>The result depends on the value of the {@link #OS_NAME} constant.
   *
   * <p>The method returns {@code false} if {@link #OS_NAME} is {@code null}.
   *
   * @param osNamePrefix the prefix for the OS name.
   * @return true if matches, or false if not or can't determine.
   */
  private static boolean getOsNameMatches(final String osNamePrefix) {
    return isOsNameMatch(OS_NAME, osNamePrefix);
  }

  /**
   * Tests whether the operating system matches with a case-insensitive comparison.
   *
   * <p>This method is package private instead of private to support unit test invocation.
   *
   * @param osName the actual OS name.
   * @param osNamePrefix the prefix for the expected OS name.
   * @return true for a case-insensitive match, or false if not.
   */
  static boolean isOsNameMatch(final String osName, final String osNamePrefix) {
    if (osName == null) {
      return false;
    }
    return Strings.CI.startsWith(osName, osNamePrefix);
  }

  /**
   * Tests if the Java version matches the version we are running.
   *
   * <p>The result depends on the value of the {@link #JAVA_SPECIFICATION_VERSION} constant.
   *
   * @param versionPrefix the prefix for the Java version.
   * @return true if matches, or false if not or can't determine.
   */
  private static boolean getJavaVersionMatches(final String versionPrefix) {
    return isJavaVersionMatch(JAVA_SPECIFICATION_VERSION, versionPrefix);
  }

  /**
   * Tests whether the Java version matches.
   *
   * <p>This method is package private instead of private to support unit test invocation.
   *
   * @param version the actual Java version.
   * @param versionPrefix the prefix for the expected Java version.
   * @return true if matches, or false if not or can't determine.
   */
  static boolean isJavaVersionMatch(final String version, final String versionPrefix) {
    if (version == null) {
      return false;
    }
    return version.startsWith(versionPrefix);
  }
}
