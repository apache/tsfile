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

package org.apache.tsfile.external.commons.lang3;

import java.util.Objects;
import java.util.function.Supplier;

public class Validate {
  private static final String DEFAULT_IS_TRUE_EX_MESSAGE = "The validated expression is false";
  private static final String DEFAULT_IS_NULL_EX_MESSAGE = "The validated object is null";

  public static void isTrue(final boolean expression) {
    if (!expression) {
      throw new IllegalArgumentException(DEFAULT_IS_TRUE_EX_MESSAGE);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Validate that the specified argument is not {@code null}; otherwise throwing an exception.
   *
   * <pre>Validate.notNull(myObject, "The object must not be null");</pre>
   *
   * <p>The message of the exception is &quot;The validated object is null&quot;.
   *
   * @param <T> the object type
   * @param object the object to check
   * @return the validated object (never {@code null} for method chaining)
   * @throws NullPointerException if the object is {@code null}
   * @see #notNull(Object, String, Object...)
   * @deprecated Use {@link Objects#requireNonNull(Object)}.
   */
  @Deprecated
  public static <T> T notNull(final T object) {
    return notNull(object, DEFAULT_IS_NULL_EX_MESSAGE);
  }

  /**
   * Validate that the specified argument is not {@code null}; otherwise throwing an exception with
   * the specified message.
   *
   * <pre>Validate.notNull(myObject, "The object must not be null");</pre>
   *
   * @param <T> the object type
   * @param object the object to check
   * @param message the {@link String#format(String, Object...)} exception message if invalid, not
   *     null
   * @param values the optional values for the formatted exception message
   * @return the validated object (never {@code null} for method chaining)
   * @throws NullPointerException if the object is {@code null}
   * @see Objects#requireNonNull(Object)
   */
  public static <T> T notNull(final T object, final String message, final Object... values) {
    return Objects.requireNonNull(object, toSupplier(message, values));
  }

  private static Supplier<String> toSupplier(final String message, final Object... values) {
    return () -> getMessage(message, values);
  }

  /**
   * Gets the message using {@link String#format(String, Object...) String.format(message, values)}
   * if the values are not empty, otherwise return the message unformatted. This method exists to
   * allow validation methods declaring a String message and varargs parameters to be used without
   * any message parameters when the message contains special characters, e.g. {@code
   * Validate.isTrue(false, "%Failed%")}.
   *
   * @param message the {@link String#format(String, Object...)} exception message if invalid, not
   *     null
   * @param values the optional values for the formatted message
   * @return formatted message using {@link String#format(String, Object...) String.format(message,
   *     values)} if the values are not empty, otherwise return the unformatted message.
   */
  private static String getMessage(final String message, final Object... values) {
    return ArrayUtils.isEmpty(values) ? message : String.format(message, values);
  }

  /**
   * Validate that the argument condition is {@code true}; otherwise throwing an exception with the
   * specified message. This method is useful when validating according to an arbitrary boolean
   * expression, such as validating a primitive number or using your own custom validation
   * expression.
   *
   * <pre>Validate.isTrue(i &gt; 0.0, "The value must be greater than zero: &#37;d", i);</pre>
   *
   * <p>For performance reasons, the long value is passed as a separate parameter and appended to
   * the exception message only in the case of an error.
   *
   * @param expression the boolean expression to check
   * @param message the {@link String#format(String, Object...)} exception message if invalid, not
   *     null
   * @param value the value to append to the message when invalid
   * @throws IllegalArgumentException if expression is {@code false}
   * @see #isTrue(boolean)
   * @see #isTrue(boolean, String, double)
   * @see #isTrue(boolean, String, Object...)
   */
  public static void isTrue(final boolean expression, final String message, final long value) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(message, Long.valueOf(value)));
    }
  }

  /**
   * Validate that the argument condition is {@code true}; otherwise throwing an exception with the
   * specified message. This method is useful when validating according to an arbitrary boolean
   * expression, such as validating a primitive number or using your own custom validation
   * expression.
   *
   * <pre>
   * Validate.isTrue(i &gt;= min &amp;&amp; i &lt;= max, "The value must be between &#37;d and &#37;d", min, max);
   * </pre>
   *
   * @param expression the boolean expression to check
   * @param message the {@link String#format(String, Object...)} exception message if invalid, not
   *     null
   * @param values the optional values for the formatted exception message, null array not
   *     recommended
   * @throws IllegalArgumentException if expression is {@code false}
   * @see #isTrue(boolean)
   * @see #isTrue(boolean, String, long)
   * @see #isTrue(boolean, String, double)
   */
  public static void isTrue(
      final boolean expression, final String message, final Object... values) {
    if (!expression) {
      throw new IllegalArgumentException(getMessage(message, values));
    }
  }
}
