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

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ObjectUtils {
  /**
   * Delegates to {@link Object#getClass()} using generics.
   *
   * @param <T> The argument type or null.
   * @param object The argument.
   * @return The argument's Class or null.
   * @since 3.13.0
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> getClass(final T object) {
    return object == null ? null : (Class<T>) object.getClass();
  }

  /**
   * Tests if an Object is not empty and not null.
   *
   * <p>The following types are supported:
   *
   * <ul>
   *   <li>{@link CharSequence}: Considered empty if its length is zero.
   *   <li>{@link Array}: Considered empty if its length is zero.
   *   <li>{@link Collection}: Considered empty if it has zero elements.
   *   <li>{@link Map}: Considered empty if it has zero key-value mappings.
   *   <li>{@link Optional}: Considered empty if {@link Optional#isPresent} returns false,
   *       regardless of the "emptiness" of the contents.
   * </ul>
   *
   * <pre>
   * ObjectUtils.isNotEmpty(null)             = false
   * ObjectUtils.isNotEmpty("")               = false
   * ObjectUtils.isNotEmpty("ab")             = true
   * ObjectUtils.isNotEmpty(new int[]{})      = false
   * ObjectUtils.isNotEmpty(new int[]{1,2,3}) = true
   * ObjectUtils.isNotEmpty(1234)             = true
   * ObjectUtils.isNotEmpty(Optional.of(""))  = true
   * ObjectUtils.isNotEmpty(Optional.empty()) = false
   * </pre>
   *
   * @param object the {@link Object} to test, may be {@code null}
   * @return {@code true} if the object has an unsupported type or is not empty and not null, {@code
   *     false} otherwise
   * @since 3.9
   */
  public static boolean isNotEmpty(final Object object) {
    return !isEmpty(object);
  }

  /**
   * Tests if an Object is empty or null.
   *
   * <p>The following types are supported:
   *
   * <ul>
   *   <li>{@link CharSequence}: Considered empty if its length is zero.
   *   <li>{@link Array}: Considered empty if its length is zero.
   *   <li>{@link Collection}: Considered empty if it has zero elements.
   *   <li>{@link Map}: Considered empty if it has zero key-value mappings.
   *   <li>{@link Optional}: Considered empty if {@link Optional#isPresent} returns false,
   *       regardless of the "emptiness" of the contents.
   * </ul>
   *
   * <pre>
   * ObjectUtils.isEmpty(null)             = true
   * ObjectUtils.isEmpty("")               = true
   * ObjectUtils.isEmpty("ab")             = false
   * ObjectUtils.isEmpty(new int[]{})      = true
   * ObjectUtils.isEmpty(new int[]{1,2,3}) = false
   * ObjectUtils.isEmpty(1234)             = false
   * ObjectUtils.isEmpty(1234)             = false
   * ObjectUtils.isEmpty(Optional.of(""))  = false
   * ObjectUtils.isEmpty(Optional.empty()) = true
   * </pre>
   *
   * @param object the {@link Object} to test, may be {@code null}
   * @return {@code true} if the object has a supported type and is empty or null, {@code false}
   *     otherwise
   * @since 3.9
   */
  public static boolean isEmpty(final Object object) {
    if (object == null) {
      return true;
    }
    if (object instanceof CharSequence) {
      return ((CharSequence) object).length() == 0;
    }
    if (isArray(object)) {
      return Array.getLength(object) == 0;
    }
    if (object instanceof Collection<?>) {
      return ((Collection<?>) object).isEmpty();
    }
    if (object instanceof Map<?, ?>) {
      return ((Map<?, ?>) object).isEmpty();
    }
    if (object instanceof Optional<?>) {
      // TODO Java 11 Use Optional#isEmpty()
      return !((Optional<?>) object).isPresent();
    }
    return false;
  }

  /**
   * Tests whether the given object is an Object array or a primitive array in a null-safe manner.
   *
   * <p>A {@code null} {@code object} Object will return {@code false}.
   *
   * <pre>
   * ObjectUtils.isArray(null)             = false
   * ObjectUtils.isArray("")               = false
   * ObjectUtils.isArray("ab")             = false
   * ObjectUtils.isArray(new int[]{})      = true
   * ObjectUtils.isArray(new int[]{1,2,3}) = true
   * ObjectUtils.isArray(1234)             = false
   * </pre>
   *
   * @param object the object to check, may be {@code null}
   * @return {@code true} if the object is an {@code array}, {@code false} otherwise
   * @since 3.13.0
   */
  public static boolean isArray(final Object object) {
    return object != null && object.getClass().isArray();
  }

  /**
   * Gets the {@code toString()} of an {@link Object} or the empty string ({@code ""}) if the input
   * is {@code null}.
   *
   * <pre>
   * ObjectUtils.toString(null)         = ""
   * ObjectUtils.toString("")           = ""
   * ObjectUtils.toString("bat")        = "bat"
   * ObjectUtils.toString(Boolean.TRUE) = "true"
   * </pre>
   *
   * @see Objects#toString(Object)
   * @see Objects#toString(Object, String)
   * @see StringUtils#defaultString(String)
   * @see String#valueOf(Object)
   * @param obj the Object to {@code toString()}, may be {@code null}.
   * @return the input's {@code toString()}, or {@code ""} if the input is {@code null}.
   * @since 2.0
   */
  public static String toString(final Object obj) {
    return Objects.toString(obj, StringUtils.EMPTY);
  }
}
