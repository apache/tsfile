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

package org.apache.tsfile.external.commons.lang3.builder;

import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.Validate;

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

public class HashCodeBuilder {
  /** Constant to use in building the hashCode. */
  private final int iConstant;

  /** Running total of the hashCode. */
  private int iTotal;

  /**
   * Two randomly chosen, odd numbers must be passed in. Ideally these should be different for each
   * class, however this is not vital.
   *
   * <p>Prime numbers are preferred, especially for the multiplier.
   *
   * @param initialOddNumber an odd number used as the initial value
   * @param multiplierOddNumber an odd number used as the multiplier
   * @throws IllegalArgumentException if the number is even
   */
  public HashCodeBuilder(final int initialOddNumber, final int multiplierOddNumber) {
    Validate.isTrue(initialOddNumber % 2 != 0, "HashCodeBuilder requires an odd initial value");
    Validate.isTrue(multiplierOddNumber % 2 != 0, "HashCodeBuilder requires an odd multiplier");
    iConstant = multiplierOddNumber;
    iTotal = initialOddNumber;
  }

  /**
   * Append a {@code hashCode} for an {@link Object}.
   *
   * @param object the Object to add to the {@code hashCode}
   * @return {@code this} instance.
   */
  public HashCodeBuilder append(final Object object) {
    if (object == null) {
      iTotal = iTotal * iConstant;

    } else if (ObjectUtils.isArray(object)) {
      // factor out array case in order to keep method small enough
      // to be inlined
      appendArray(object);
    } else {
      iTotal = iTotal * iConstant + object.hashCode();
    }
    return this;
  }

  /**
   * Append a {@code hashCode} for an array.
   *
   * @param object the array to add to the {@code hashCode}
   */
  private void appendArray(final Object object) {
    // 'Switch' on type of array, to dispatch to the correct handler
    // This handles multidimensional arrays
    if (object instanceof long[]) {
      append((long[]) object);
    } else if (object instanceof int[]) {
      append((int[]) object);
    } else if (object instanceof short[]) {
      append((short[]) object);
    } else if (object instanceof char[]) {
      append((char[]) object);
    } else if (object instanceof byte[]) {
      append((byte[]) object);
    } else if (object instanceof double[]) {
      append((double[]) object);
    } else if (object instanceof float[]) {
      append((float[]) object);
    } else if (object instanceof boolean[]) {
      append((boolean[]) object);
    } else {
      // Not an array of primitives
      append((Object[]) object);
    }
  }

  /**
   * Returns the computed {@code hashCode}.
   *
   * @return {@code hashCode} based on the fields appended
   */
  public int toHashCode() {
    return iTotal;
  }
}
