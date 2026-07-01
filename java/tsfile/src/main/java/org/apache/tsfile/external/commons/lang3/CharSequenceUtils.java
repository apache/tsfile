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

public class CharSequenceUtils {

  private static final int NOT_FOUND = -1;

  /**
   * Returns the index within {@code cs} of the first occurrence of the specified character,
   * starting the search at the specified index.
   *
   * <p>If a character with value {@code searchChar} occurs in the character sequence represented by
   * the {@code cs} object at an index no smaller than {@code start}, then the index of the first
   * such occurrence is returned. For values of {@code searchChar} in the range from 0 to 0xFFFF
   * (inclusive), this is the smallest value <i>k</i> such that:
   *
   * <blockquote>
   *
   * <pre>
   * (this.charAt(<i>k</i>) == searchChar) &amp;&amp; (<i>k</i> &gt;= start)
   * </pre>
   *
   * </blockquote>
   *
   * is true. For other values of {@code searchChar}, it is the smallest value <i>k</i> such that:
   *
   * <blockquote>
   *
   * <pre>
   * (this.codePointAt(<i>k</i>) == searchChar) &amp;&amp; (<i>k</i> &gt;= start)
   * </pre>
   *
   * </blockquote>
   *
   * <p>is true. In either case, if no such character occurs inm {@code cs} at or after position
   * {@code start}, then {@code -1} is returned.
   *
   * <p>There is no restriction on the value of {@code start}. If it is negative, it has the same
   * effect as if it were zero: the entire {@link CharSequence} may be searched. If it is greater
   * than the length of {@code cs}, it has the same effect as if it were equal to the length of
   * {@code cs}: {@code -1} is returned.
   *
   * <p>All indices are specified in {@code char} values (Unicode code units).
   *
   * @param cs the {@link CharSequence} to be processed, not null
   * @param searchChar the char to be searched for
   * @param start the start index, negative starts at the string start
   * @return the index where the search char was found, -1 if not found
   * @since 3.6 updated to behave more like {@link String}
   */
  static int indexOf(final CharSequence cs, final int searchChar, int start) {
    if (cs instanceof String) {
      return ((String) cs).indexOf(searchChar, start);
    }
    final int sz = cs.length();
    if (start < 0) {
      start = 0;
    }
    if (searchChar < Character.MIN_SUPPLEMENTARY_CODE_POINT) {
      for (int i = start; i < sz; i++) {
        if (cs.charAt(i) == searchChar) {
          return i;
        }
      }
      return NOT_FOUND;
    }
    // supplementary characters (LANG1300)
    if (searchChar <= Character.MAX_CODE_POINT) {
      final char[] chars = Character.toChars(searchChar);
      for (int i = start; i < sz - 1; i++) {
        final char high = cs.charAt(i);
        final char low = cs.charAt(i + 1);
        if (high == chars[0] && low == chars[1]) {
          return i;
        }
      }
    }
    return NOT_FOUND;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Green implementation of regionMatches.
   *
   * @param cs the {@link CharSequence} to be processed
   * @param ignoreCase whether or not to be case-insensitive
   * @param thisStart the index to start on the {@code cs} CharSequence
   * @param substring the {@link CharSequence} to be looked for
   * @param start the index to start on the {@code substring} CharSequence
   * @param length character length of the region
   * @return whether the region matched
   * @see String#regionMatches(boolean, int, String, int, int)
   */
  static boolean regionMatches(
      final CharSequence cs,
      final boolean ignoreCase,
      final int thisStart,
      final CharSequence substring,
      final int start,
      final int length) {
    if (cs instanceof String && substring instanceof String) {
      return ((String) cs).regionMatches(ignoreCase, thisStart, (String) substring, start, length);
    }
    int index1 = thisStart;
    int index2 = start;
    int tmpLen = length;

    // Extract these first so we detect NPEs the same as the java.lang.String version
    final int srcLen = cs.length() - thisStart;
    final int otherLen = substring.length() - start;

    // Check for invalid parameters
    if (thisStart < 0 || start < 0 || length < 0) {
      return false;
    }

    // Check that the regions are long enough
    if (srcLen < length || otherLen < length) {
      return false;
    }

    while (tmpLen-- > 0) {
      final char c1 = cs.charAt(index1++);
      final char c2 = substring.charAt(index2++);

      if (c1 == c2) {
        continue;
      }

      if (!ignoreCase) {
        return false;
      }

      // The real same check as in String#regionMatches(boolean, int, String, int, int):
      final char u1 = Character.toUpperCase(c1);
      final char u2 = Character.toUpperCase(c2);
      if (u1 != u2 && Character.toLowerCase(u1) != Character.toLowerCase(u2)) {
        return false;
      }
    }

    return true;
  }
}
