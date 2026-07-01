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

import java.util.Arrays;

public class ArrayFill {
  /**
   * Fills and returns the given array, assigning the given {@code char} value to each element of
   * the array.
   *
   * @param a the array to be filled (may be null).
   * @param val the value to be stored in all elements of the array.
   * @return the given array.
   * @see Arrays#fill(char[],char)
   */
  public static char[] fill(final char[] a, final char val) {
    if (a != null) {
      Arrays.fill(a, val);
    }
    return a;
  }
}
