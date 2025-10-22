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

public abstract class Strings {
  /** The <strong>C</strong>ase-<strong>I</strong>nsensitive singleton instance. */
  public static final Strings CI = new CiStrings(true);

  /**
   * Represents a failed index search.
   *
   * @since 2.1
   */
  public static final int INDEX_NOT_FOUND = -1;

  /** Ignores case when possible. */
  private final boolean ignoreCase;

  /** Compares null as less when possible. */
  private final boolean nullIsLess;

  /**
   * Constructs a new instance.
   *
   * @param ignoreCase Ignores case when possible.
   * @param nullIsLess Compares null as less when possible.
   */
  private Strings(final boolean ignoreCase, final boolean nullIsLess) {
    this.ignoreCase = ignoreCase;
    this.nullIsLess = nullIsLess;
  }

  /**
   * Compare two Strings lexicographically, like {@link String#compareTo(String)}.
   *
   * <p>The return values are:
   *
   * <ul>
   *   <li>{@code int = 0}, if {@code str1} is equal to {@code str2} (or both {@code null})
   *   <li>{@code int < 0}, if {@code str1} is less than {@code str2}
   *   <li>{@code int > 0}, if {@code str1} is greater than {@code str2}
   * </ul>
   *
   * <p>This is a {@code null} safe version of :
   *
   * <pre>
   * str1.compareTo(str2)
   * </pre>
   *
   * <p>{@code null} value is considered less than non-{@code null} value. Two {@code null}
   * references are considered equal.
   *
   * <p>Case-sensitive examples
   *
   * <pre>{@code
   * Strings.CS.compare(null, null)   = 0
   * Strings.CS.compare(null , "a")   < 0
   * Strings.CS.compare("a", null)   > 0
   * Strings.CS.compare("abc", "abc") = 0
   * Strings.CS.compare("a", "b")     < 0
   * Strings.CS.compare("b", "a")     > 0
   * Strings.CS.compare("a", "B")     > 0
   * Strings.CS.compare("ab", "abc")  < 0
   * }</pre>
   *
   * <p>Case-insensitive examples
   *
   * <pre>{@code
   * Strings.CI.compareIgnoreCase(null, null)   = 0
   * Strings.CI.compareIgnoreCase(null , "a")   < 0
   * Strings.CI.compareIgnoreCase("a", null)    > 0
   * Strings.CI.compareIgnoreCase("abc", "abc") = 0
   * Strings.CI.compareIgnoreCase("abc", "ABC") = 0
   * Strings.CI.compareIgnoreCase("a", "b")     < 0
   * Strings.CI.compareIgnoreCase("b", "a")     > 0
   * Strings.CI.compareIgnoreCase("a", "B")     < 0
   * Strings.CI.compareIgnoreCase("A", "b")     < 0
   * Strings.CI.compareIgnoreCase("ab", "ABC")  < 0
   * }</pre>
   *
   * @see String#compareTo(String)
   * @param str1 the String to compare from
   * @param str2 the String to compare to
   * @return &lt; 0, 0, &gt; 0, if {@code str1} is respectively less, equal or greater than {@code
   *     str2}
   */
  public abstract int compare(String str1, String str2);

  /**
   * Tests if CharSequence contains a search CharSequence, handling {@code null}. This method uses
   * {@link String#indexOf(String)} if possible.
   *
   * <p>A {@code null} CharSequence will return {@code false}.
   *
   * <p>Case-sensitive examples
   *
   * <pre>
   * Strings.CS.contains(null, *)     = false
   * Strings.CS.contains(*, null)     = false
   * Strings.CS.contains("", "")      = true
   * Strings.CS.contains("abc", "")   = true
   * Strings.CS.contains("abc", "a")  = true
   * Strings.CS.contains("abc", "z")  = false
   * </pre>
   *
   * <p>Case-insensitive examples
   *
   * <pre>
   * Strings.CI.containsIgnoreCase(null, *)    = false
   * Strings.CI.containsIgnoreCase(*, null)    = false
   * Strings.CI.containsIgnoreCase("", "")     = true
   * Strings.CI.containsIgnoreCase("abc", "")  = true
   * Strings.CI.containsIgnoreCase("abc", "a") = true
   * Strings.CI.containsIgnoreCase("abc", "z") = false
   * Strings.CI.containsIgnoreCase("abc", "A") = true
   * Strings.CI.containsIgnoreCase("abc", "Z") = false
   * </pre>
   *
   * @param seq the CharSequence to check, may be null
   * @param searchSeq the CharSequence to find, may be null
   * @return true if the CharSequence contains the search CharSequence, false if not or {@code null}
   *     string input
   */
  public abstract boolean contains(CharSequence seq, CharSequence searchSeq);

  /**
   * Compares two CharSequences, returning {@code true} if they represent equal sequences of
   * characters.
   *
   * <p>{@code null}s are handled without exceptions. Two {@code null} references are considered to
   * be equal.
   *
   * <p>Case-sensitive examples
   *
   * <pre>
   * Strings.CS.equals(null, null)   = true
   * Strings.CS.equals(null, "abc")  = false
   * Strings.CS.equals("abc", null)  = false
   * Strings.CS.equals("abc", "abc") = true
   * Strings.CS.equals("abc", "ABC") = false
   * </pre>
   *
   * <p>Case-insensitive examples
   *
   * <pre>
   * Strings.CI.equalsIgnoreCase(null, null)   = true
   * Strings.CI.equalsIgnoreCase(null, "abc")  = false
   * Strings.CI.equalsIgnoreCase("abc", null)  = false
   * Strings.CI.equalsIgnoreCase("abc", "abc") = true
   * Strings.CI.equalsIgnoreCase("abc", "ABC") = true
   * </pre>
   *
   * @param cs1 the first CharSequence, may be {@code null}
   * @param cs2 the second CharSequence, may be {@code null}
   * @return {@code true} if the CharSequences are equal (case-sensitive), or both {@code null}
   * @see Object#equals(Object)
   * @see String#compareTo(String)
   * @see String#equalsIgnoreCase(String)
   */
  public abstract boolean equals(CharSequence cs1, CharSequence cs2);

  /**
   * Compares two CharSequences, returning {@code true} if they represent equal sequences of
   * characters.
   *
   * <p>{@code null}s are handled without exceptions. Two {@code null} references are considered to
   * be equal.
   *
   * <p>Case-sensitive examples
   *
   * <pre>
   * Strings.CS.equals(null, null)   = true
   * Strings.CS.equals(null, "abc")  = false
   * Strings.CS.equals("abc", null)  = false
   * Strings.CS.equals("abc", "abc") = true
   * Strings.CS.equals("abc", "ABC") = false
   * </pre>
   *
   * <p>Case-insensitive examples
   *
   * <pre>
   * Strings.CI.equalsIgnoreCase(null, null)   = true
   * Strings.CI.equalsIgnoreCase(null, "abc")  = false
   * Strings.CI.equalsIgnoreCase("abc", null)  = false
   * Strings.CI.equalsIgnoreCase("abc", "abc") = true
   * Strings.CI.equalsIgnoreCase("abc", "ABC") = true
   * </pre>
   *
   * @param str1 the first CharSequence, may be {@code null}
   * @param str2 the second CharSequence, may be {@code null}
   * @return {@code true} if the CharSequences are equal (case-sensitive), or both {@code null}
   * @see Object#equals(Object)
   * @see String#compareTo(String)
   * @see String#equalsIgnoreCase(String)
   */
  public abstract boolean equals(String str1, String str2);

  /**
   * Finds the first index within a CharSequence, handling {@code null}. This method uses {@link
   * String#indexOf(String, int)} if possible.
   *
   * <p>A {@code null} CharSequence will return {@code -1}. A negative start position is treated as
   * zero. An empty ("") search CharSequence always matches. A start position greater than the
   * string length only matches an empty search CharSequence.
   *
   * <p>Case-sensitive examples
   *
   * <pre>
   * Strings.CS.indexOf(null, *, *)          = -1
   * Strings.CS.indexOf(*, null, *)          = -1
   * Strings.CS.indexOf("", "", 0)           = 0
   * Strings.CS.indexOf("", *, 0)            = -1 (except when * = "")
   * Strings.CS.indexOf("aabaabaa", "a", 0)  = 0
   * Strings.CS.indexOf("aabaabaa", "b", 0)  = 2
   * Strings.CS.indexOf("aabaabaa", "ab", 0) = 1
   * Strings.CS.indexOf("aabaabaa", "b", 3)  = 5
   * Strings.CS.indexOf("aabaabaa", "b", 9)  = -1
   * Strings.CS.indexOf("aabaabaa", "b", -1) = 2
   * Strings.CS.indexOf("aabaabaa", "", 2)   = 2
   * Strings.CS.indexOf("abc", "", 9)        = 3
   * </pre>
   *
   * <p>Case-insensitive examples
   *
   * <pre>
   * Strings.CI.indexOfIgnoreCase(null, *, *)          = -1
   * Strings.CI.indexOfIgnoreCase(*, null, *)          = -1
   * Strings.CI.indexOfIgnoreCase("", "", 0)           = 0
   * Strings.CI.indexOfIgnoreCase("aabaabaa", "A", 0)  = 0
   * Strings.CI.indexOfIgnoreCase("aabaabaa", "B", 0)  = 2
   * Strings.CI.indexOfIgnoreCase("aabaabaa", "AB", 0) = 1
   * Strings.CI.indexOfIgnoreCase("aabaabaa", "B", 3)  = 5
   * Strings.CI.indexOfIgnoreCase("aabaabaa", "B", 9)  = -1
   * Strings.CI.indexOfIgnoreCase("aabaabaa", "B", -1) = 2
   * Strings.CI.indexOfIgnoreCase("aabaabaa", "", 2)   = 2
   * Strings.CI.indexOfIgnoreCase("abc", "", 9)        = -1
   * </pre>
   *
   * @param seq the CharSequence to check, may be null
   * @param searchSeq the CharSequence to find, may be null
   * @param startPos the start position, negative treated as zero
   * @return the first index of the search CharSequence (always &ge; startPos), -1 if no match or
   *     {@code null} string input
   */
  public abstract int indexOf(CharSequence seq, CharSequence searchSeq, int startPos);

  /**
   * Finds the last index within a CharSequence, handling {@code null}. This method uses {@link
   * String#lastIndexOf(String, int)} if possible.
   *
   * <p>A {@code null} CharSequence will return {@code -1}. A negative start position returns {@code
   * -1}. An empty ("") search CharSequence always matches unless the start position is negative. A
   * start position greater than the string length searches the whole string. The search starts at
   * the startPos and works backwards; matches starting after the start position are ignored.
   *
   * <p>Case-sensitive examples
   *
   * <pre>
   * Strings.CS.lastIndexOf(null, *, *)          = -1
   * Strings.CS.lastIndexOf(*, null, *)          = -1
   * Strings.CS.lastIndexOf("aabaabaa", "a", 8)  = 7
   * Strings.CS.lastIndexOf("aabaabaa", "b", 8)  = 5
   * Strings.CS.lastIndexOf("aabaabaa", "ab", 8) = 4
   * Strings.CS.lastIndexOf("aabaabaa", "b", 9)  = 5
   * Strings.CS.lastIndexOf("aabaabaa", "b", -1) = -1
   * Strings.CS.lastIndexOf("aabaabaa", "a", 0)  = 0
   * Strings.CS.lastIndexOf("aabaabaa", "b", 0)  = -1
   * Strings.CS.lastIndexOf("aabaabaa", "b", 1)  = -1
   * Strings.CS.lastIndexOf("aabaabaa", "b", 2)  = 2
   * Strings.CS.lastIndexOf("aabaabaa", "ba", 2)  = 2
   * </pre>
   *
   * <p>Case-insensitive examples
   *
   * <pre>
   * Strings.CI.lastIndexOfIgnoreCase(null, *, *)          = -1
   * Strings.CI.lastIndexOfIgnoreCase(*, null, *)          = -1
   * Strings.CI.lastIndexOfIgnoreCase("aabaabaa", "A", 8)  = 7
   * Strings.CI.lastIndexOfIgnoreCase("aabaabaa", "B", 8)  = 5
   * Strings.CI.lastIndexOfIgnoreCase("aabaabaa", "AB", 8) = 4
   * Strings.CI.lastIndexOfIgnoreCase("aabaabaa", "B", 9)  = 5
   * Strings.CI.lastIndexOfIgnoreCase("aabaabaa", "B", -1) = -1
   * Strings.CI.lastIndexOfIgnoreCase("aabaabaa", "A", 0)  = 0
   * Strings.CI.lastIndexOfIgnoreCase("aabaabaa", "B", 0)  = -1
   * </pre>
   *
   * @param seq the CharSequence to check, may be null
   * @param searchSeq the CharSequence to find, may be null
   * @param startPos the start position, negative treated as zero
   * @return the last index of the search CharSequence (always &le; startPos), -1 if no match or
   *     {@code null} string input
   */
  public abstract int lastIndexOf(CharSequence seq, CharSequence searchSeq, int startPos);

  /**
   * Tests whether null is less when comparing.
   *
   * @return whether null is less when comparing.
   */
  boolean isNullIsLess() {
    return nullIsLess;
  }

  /**
   * Tests if a CharSequence starts with a specified prefix.
   *
   * <p>{@code null}s are handled without exceptions. Two {@code null} references are considered to
   * be equal.
   *
   * <p>Case-sensitive examples
   *
   * <pre>
   * Strings.CS.startsWith(null, null)      = true
   * Strings.CS.startsWith(null, "abc")     = false
   * Strings.CS.startsWith("abcdef", null)  = false
   * Strings.CS.startsWith("abcdef", "abc") = true
   * Strings.CS.startsWith("ABCDEF", "abc") = false
   * </pre>
   *
   * <p>Case-insensitive examples
   *
   * <pre>
   * Strings.CI.startsWithIgnoreCase(null, null)      = true
   * Strings.CI.startsWithIgnoreCase(null, "abc")     = false
   * Strings.CI.startsWithIgnoreCase("abcdef", null)  = false
   * Strings.CI.startsWithIgnoreCase("abcdef", "abc") = true
   * Strings.CI.startsWithIgnoreCase("ABCDEF", "abc") = true
   * </pre>
   *
   * @see String#startsWith(String)
   * @param str the CharSequence to check, may be null
   * @param prefix the prefix to find, may be null
   * @return {@code true} if the CharSequence starts with the prefix, case-sensitive, or both {@code
   *     null}
   */
  public boolean startsWith(final CharSequence str, final CharSequence prefix) {
    if (str == null || prefix == null) {
      return str == prefix;
    }
    final int preLen = prefix.length();
    if (preLen > str.length()) {
      return false;
    }
    return CharSequenceUtils.regionMatches(str, ignoreCase, 0, prefix, 0, preLen);
  }

  private static final class CiStrings extends Strings {

    private CiStrings(final boolean nullIsLess) {
      super(true, nullIsLess);
    }

    @Override
    public int compare(final String s1, final String s2) {
      if (s1 == s2) {
        // Both null or same object
        return 0;
      }
      if (s1 == null) {
        return isNullIsLess() ? -1 : 1;
      }
      if (s2 == null) {
        return isNullIsLess() ? 1 : -1;
      }
      return s1.compareToIgnoreCase(s2);
    }

    @Override
    public boolean contains(final CharSequence str, final CharSequence searchStr) {
      if (str == null || searchStr == null) {
        return false;
      }
      final int len = searchStr.length();
      final int max = str.length() - len;
      for (int i = 0; i <= max; i++) {
        if (CharSequenceUtils.regionMatches(str, true, i, searchStr, 0, len)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean equals(final CharSequence cs1, final CharSequence cs2) {
      if (cs1 == cs2) {
        return true;
      }
      if (cs1 == null || cs2 == null) {
        return false;
      }
      if (cs1.length() != cs2.length()) {
        return false;
      }
      return CharSequenceUtils.regionMatches(cs1, true, 0, cs2, 0, cs1.length());
    }

    @Override
    public boolean equals(final String s1, final String s2) {
      return s1 == null ? s2 == null : s1.equalsIgnoreCase(s2);
    }

    @Override
    public int indexOf(final CharSequence str, final CharSequence searchStr, int startPos) {
      if (str == null || searchStr == null) {
        return INDEX_NOT_FOUND;
      }
      if (startPos < 0) {
        startPos = 0;
      }
      final int endLimit = str.length() - searchStr.length() + 1;
      if (startPos > endLimit) {
        return INDEX_NOT_FOUND;
      }
      if (searchStr.length() == 0) {
        return startPos;
      }
      for (int i = startPos; i < endLimit; i++) {
        if (CharSequenceUtils.regionMatches(str, true, i, searchStr, 0, searchStr.length())) {
          return i;
        }
      }
      return INDEX_NOT_FOUND;
    }

    @Override
    public int lastIndexOf(final CharSequence str, final CharSequence searchStr, int startPos) {
      if (str == null || searchStr == null) {
        return INDEX_NOT_FOUND;
      }
      final int searchStrLength = searchStr.length();
      final int strLength = str.length();
      if (startPos > strLength - searchStrLength) {
        startPos = strLength - searchStrLength;
      }
      if (startPos < 0) {
        return INDEX_NOT_FOUND;
      }
      if (searchStrLength == 0) {
        return startPos;
      }
      for (int i = startPos; i >= 0; i--) {
        if (CharSequenceUtils.regionMatches(str, true, i, searchStr, 0, searchStrLength)) {
          return i;
        }
      }
      return INDEX_NOT_FOUND;
    }
  }
}
