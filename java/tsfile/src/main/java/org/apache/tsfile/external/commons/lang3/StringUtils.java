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

import org.apache.tsfile.external.commons.lang3.function.Suppliers;
import org.apache.tsfile.external.commons.lang3.stream.LangCollectors;
import org.apache.tsfile.external.commons.lang3.stream.Streams;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class StringUtils {

  /**
   * Checks if a CharSequence is empty ("") or null.
   *
   * <pre>
   * StringUtils.isEmpty(null)      = true
   * StringUtils.isEmpty("")        = true
   * StringUtils.isEmpty(" ")       = false
   * StringUtils.isEmpty("bob")     = false
   * StringUtils.isEmpty("  bob  ") = false
   * </pre>
   *
   * <p>NOTE: This method changed in Lang version 2.0. It no longer trims the CharSequence. That
   * functionality is available in isBlank().
   *
   * @param cs the CharSequence to check, may be null
   * @return {@code true} if the CharSequence is empty or null
   * @since 3.0 Changed signature from isEmpty(String) to isEmpty(CharSequence)
   */
  public static boolean isEmpty(final CharSequence cs) {
    return cs == null || cs.length() == 0;
  }

  /**
   * Checks if a CharSequence is empty (""), null or whitespace only.
   *
   * <p>Whitespace is defined by {@link Character#isWhitespace(char)}.
   *
   * <pre>
   * StringUtils.isBlank(null)      = true
   * StringUtils.isBlank("")        = true
   * StringUtils.isBlank(" ")       = true
   * StringUtils.isBlank("bob")     = false
   * StringUtils.isBlank("  bob  ") = false
   * </pre>
   *
   * @param cs the CharSequence to check, may be null
   * @return {@code true} if the CharSequence is null, empty or whitespace only
   * @since 2.0
   * @since 3.0 Changed signature from isBlank(String) to isBlank(CharSequence)
   */
  public static boolean isBlank(final CharSequence cs) {
    final int strLen = length(cs);
    if (strLen == 0) {
      return true;
    }
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(cs.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if CharSequence contains a search character, handling {@code null}. This method uses
   * {@link String#indexOf(int)} if possible.
   *
   * <p>A {@code null} or empty ("") CharSequence will return {@code false}.
   *
   * <pre>
   * StringUtils.contains(null, *)    = false
   * StringUtils.contains("", *)      = false
   * StringUtils.contains("abc", 'a') = true
   * StringUtils.contains("abc", 'z') = false
   * </pre>
   *
   * @param seq the CharSequence to check, may be null
   * @param searchChar the character to find
   * @return true if the CharSequence contains the search character, false if not or {@code null}
   *     string input
   * @since 2.0
   * @since 3.0 Changed signature from contains(String, int) to contains(CharSequence, int)
   */
  public static boolean contains(final CharSequence seq, final int searchChar) {
    if (isEmpty(seq)) {
      return false;
    }
    return CharSequenceUtils.indexOf(seq, searchChar, 0) >= 0;
  }

  public static int length(final CharSequence cs) {
    return cs == null ? 0 : cs.length();
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * The empty String {@code ""}.
   *
   * @since 2.0
   */
  public static final String EMPTY = "";

  /** The maximum size to which the padding constant(s) can expand. */
  private static final int PAD_LIMIT = 8192;

  /**
   * A String for a space character.
   *
   * @since 3.2
   */
  public static final String SPACE = " ";

  /**
   * Repeats a String {@code repeat} times to form a new String.
   *
   * <pre>
   * StringUtils.repeat(null, 2) = null
   * StringUtils.repeat("", 0)   = ""
   * StringUtils.repeat("", 2)   = ""
   * StringUtils.repeat("a", 3)  = "aaa"
   * StringUtils.repeat("ab", 2) = "abab"
   * StringUtils.repeat("a", -2) = ""
   * </pre>
   *
   * @param repeat the String to repeat, may be null
   * @param count number of times to repeat str, negative treated as zero
   * @return a new String consisting of the original String repeated, {@code null} if null String
   *     input
   */
  public static String repeat(final String repeat, final int count) {
    // Performance tuned for 2.0 (JDK1.4)
    if (repeat == null) {
      return null;
    }
    if (count <= 0) {
      return EMPTY;
    }
    final int inputLength = repeat.length();
    if (count == 1 || inputLength == 0) {
      return repeat;
    }
    if (inputLength == 1 && count <= PAD_LIMIT) {
      return repeat(repeat.charAt(0), count);
    }

    final int outputLength = inputLength * count;
    switch (inputLength) {
      case 1:
        return repeat(repeat.charAt(0), count);
      case 2:
        final char ch0 = repeat.charAt(0);
        final char ch1 = repeat.charAt(1);
        final char[] output2 = new char[outputLength];
        for (int i = count * 2 - 2; i >= 0; i--, i--) {
          output2[i] = ch0;
          output2[i + 1] = ch1;
        }
        return new String(output2);
      default:
        final StringBuilder buf = new StringBuilder(outputLength);
        for (int i = 0; i < count; i++) {
          buf.append(repeat);
        }
        return buf.toString();
    }
  }

  /**
   * Returns padding using the specified delimiter repeated to a given length.
   *
   * <pre>
   * StringUtils.repeat('e', 0)  = ""
   * StringUtils.repeat('e', 3)  = "eee"
   * StringUtils.repeat('e', -2) = ""
   * </pre>
   *
   * <p>Note: this method does not support padding with <a
   * href="https://www.unicode.org/glossary/#supplementary_character">Unicode Supplementary
   * Characters</a> as they require a pair of {@code char}s to be represented. If you are needing to
   * support full I18N of your applications consider using {@link #repeat(String, int)} instead.
   *
   * @param repeat character to repeat
   * @param count number of times to repeat char, negative treated as zero
   * @return String with repeated character
   * @see #repeat(String, int)
   */
  public static String repeat(final char repeat, final int count) {
    if (count <= 0) {
      return EMPTY;
    }
    return new String(ArrayFill.fill(new char[count], repeat));
  }

  /**
   * Joins the elements of the provided array into a single String containing the provided list of
   * elements.
   *
   * <p>No delimiter is added before or after the list. Null objects or empty strings within the
   * array are represented by empty strings.
   *
   * <pre>
   * StringUtils.join(null, *)               = null
   * StringUtils.join([], *)                 = ""
   * StringUtils.join([null], *)             = ""
   * StringUtils.join([1, 2, 3], ';')  = "1;2;3"
   * StringUtils.join([1, 2, 3], null) = "123"
   * </pre>
   *
   * @param array the array of values to join together, may be null
   * @param separator the separator character to use
   * @return the joined String, {@code null} if null array input
   * @since 3.2
   */
  public static String join(final int[] array, final char separator) {
    if (array == null) {
      return null;
    }
    return join(array, separator, 0, array.length);
  }

  /**
   * Joins the elements of the provided array into a single String containing the provided list of
   * elements.
   *
   * <p>No delimiter is added before or after the list. Null objects or empty strings within the
   * array are represented by empty strings.
   *
   * <pre>
   * StringUtils.join(null, *)               = null
   * StringUtils.join([], *)                 = ""
   * StringUtils.join([null], *)             = ""
   * StringUtils.join([1, 2, 3], ';')  = "1;2;3"
   * StringUtils.join([1, 2, 3], null) = "123"
   * </pre>
   *
   * @param array the array of values to join together, may be null
   * @param delimiter the separator character to use
   * @param startIndex the first index to start joining from. It is an error to pass in a start
   *     index past the end of the array
   * @param endIndex the index to stop joining from (exclusive). It is an error to pass in an end
   *     index past the end of the array
   * @return the joined String, {@code null} if null array input
   * @since 3.2
   */
  public static String join(
      final int[] array, final char delimiter, final int startIndex, final int endIndex) {
    if (array == null) {
      return null;
    }
    if (endIndex - startIndex <= 0) {
      return EMPTY;
    }
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = startIndex; i < endIndex; i++) {
      stringBuilder.append(array[i]).append(delimiter);
    }
    return stringBuilder.substring(0, stringBuilder.length() - 1);
  }

  /**
   * Tests if a CharSequence is not {@link #isBlank(CharSequence) blank} (whitespaces, empty ({@code
   * ""}) or {@code null}).
   *
   * <p>Whitespace is defined by {@link Character#isWhitespace(char)}.
   *
   * <pre>
   * StringUtils.isNotBlank(null)      = false
   * StringUtils.isNotBlank("")        = false
   * StringUtils.isNotBlank(" ")       = false
   * StringUtils.isNotBlank("bob")     = true
   * StringUtils.isNotBlank("  bob  ") = true
   * </pre>
   *
   * @param cs the CharSequence to check, may be null
   * @return {@code true} if the CharSequence is not {@link #isBlank(CharSequence) blank}
   *     (whitespaces, empty ({@code ""}) or {@code null})
   * @see #isBlank(CharSequence)
   * @since 2.0
   * @since 3.0 Changed signature from isNotBlank(String) to isNotBlank(CharSequence)
   */
  public static boolean isNotBlank(final CharSequence cs) {
    return !isBlank(cs);
  }

  /**
   * Removes control characters (char &lt;= 32) from both ends of this String, handling {@code null}
   * by returning {@code null}.
   *
   * <p>The String is trimmed using {@link String#trim()}. Trim removes start and end characters
   * &lt;= 32. To strip whitespace use {@link #strip(String)}.
   *
   * <p>To trim your choice of characters, use the {@link #strip(String, String)} methods.
   *
   * <pre>
   * StringUtils.trim(null)          = null
   * StringUtils.trim("")            = ""
   * StringUtils.trim("     ")       = ""
   * StringUtils.trim("abc")         = "abc"
   * StringUtils.trim("    abc    ") = "abc"
   * </pre>
   *
   * @param str the String to be trimmed, may be null
   * @return the trimmed string, {@code null} if null String input
   */
  public static String trim(final String str) {
    return str == null ? null : str.trim();
  }

  /**
   * Splits the provided text into an array, separators specified, preserving all tokens, including
   * empty tokens created by adjacent separators. This is an alternative to using StringTokenizer.
   *
   * <p>The separator is not included in the returned String array. Adjacent separators are treated
   * as separators for empty tokens. For more control over the split use the StrTokenizer class.
   *
   * <p>A {@code null} input String returns {@code null}. A {@code null} separatorChars splits on
   * whitespace.
   *
   * <pre>
   * StringUtils.splitPreserveAllTokens(null, *)           = null
   * StringUtils.splitPreserveAllTokens("", *)             = []
   * StringUtils.splitPreserveAllTokens("abc def", null)   = ["abc", "def"]
   * StringUtils.splitPreserveAllTokens("abc def", " ")    = ["abc", "def"]
   * StringUtils.splitPreserveAllTokens("abc  def", " ")   = ["abc", "", "def"]
   * StringUtils.splitPreserveAllTokens("ab:cd:ef", ":")   = ["ab", "cd", "ef"]
   * StringUtils.splitPreserveAllTokens("ab:cd:ef:", ":")  = ["ab", "cd", "ef", ""]
   * StringUtils.splitPreserveAllTokens("ab:cd:ef::", ":") = ["ab", "cd", "ef", "", ""]
   * StringUtils.splitPreserveAllTokens("ab::cd:ef", ":")  = ["ab", "", "cd", "ef"]
   * StringUtils.splitPreserveAllTokens(":cd:ef", ":")     = ["", "cd", "ef"]
   * StringUtils.splitPreserveAllTokens("::cd:ef", ":")    = ["", "", "cd", "ef"]
   * StringUtils.splitPreserveAllTokens(":cd:ef:", ":")    = ["", "cd", "ef", ""]
   * </pre>
   *
   * @param str the String to parse, may be {@code null}
   * @param separatorChars the characters used as the delimiters, {@code null} splits on whitespace
   * @return an array of parsed Strings, {@code null} if null String input
   * @since 2.1
   */
  public static String[] splitPreserveAllTokens(final String str, final String separatorChars) {
    return splitWorker(str, separatorChars, -1, true);
  }

  /**
   * Performs the logic for the {@code split} and {@code splitPreserveAllTokens} methods that return
   * a maximum array length.
   *
   * @param str the String to parse, may be {@code null}
   * @param separatorChars the separate character
   * @param max the maximum number of elements to include in the array. A zero or negative value
   *     implies no limit.
   * @param preserveAllTokens if {@code true}, adjacent separators are treated as empty token
   *     separators; if {@code false}, adjacent separators are treated as one separator.
   * @return an array of parsed Strings, {@code null} if null String input
   */
  private static String[] splitWorker(
      final String str,
      final String separatorChars,
      final int max,
      final boolean preserveAllTokens) {
    // Performance tuned for 2.0 (JDK1.4)
    // Direct code is quicker than StringTokenizer.
    // Also, StringTokenizer uses isSpace() not isWhitespace()

    if (str == null) {
      return null;
    }
    final int len = str.length();
    if (len == 0) {
      return ArrayUtils.EMPTY_STRING_ARRAY;
    }
    final List<String> list = new ArrayList<>();
    int sizePlus1 = 1;
    int i = 0;
    int start = 0;
    boolean match = false;
    boolean lastMatch = false;
    if (separatorChars == null) {
      // Null separator means use whitespace
      while (i < len) {
        if (Character.isWhitespace(str.charAt(i))) {
          if (match || preserveAllTokens) {
            lastMatch = true;
            if (sizePlus1++ == max) {
              i = len;
              lastMatch = false;
            }
            list.add(str.substring(start, i));
            match = false;
          }
          start = ++i;
          continue;
        }
        lastMatch = false;
        match = true;
        i++;
      }
    } else if (separatorChars.length() == 1) {
      // Optimize 1 character case
      final char sep = separatorChars.charAt(0);
      while (i < len) {
        if (str.charAt(i) == sep) {
          if (match || preserveAllTokens) {
            lastMatch = true;
            if (sizePlus1++ == max) {
              i = len;
              lastMatch = false;
            }
            list.add(str.substring(start, i));
            match = false;
          }
          start = ++i;
          continue;
        }
        lastMatch = false;
        match = true;
        i++;
      }
    } else {
      // standard case
      while (i < len) {
        if (separatorChars.indexOf(str.charAt(i)) >= 0) {
          if (match || preserveAllTokens) {
            lastMatch = true;
            if (sizePlus1++ == max) {
              i = len;
              lastMatch = false;
            }
            list.add(str.substring(start, i));
            match = false;
          }
          start = ++i;
          continue;
        }
        lastMatch = false;
        match = true;
        i++;
      }
    }
    if (match || preserveAllTokens && lastMatch) {
      list.add(str.substring(start, i));
    }
    return list.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
  }

  /**
   * Returns either the passed in CharSequence, or if the CharSequence is empty or {@code null}, the
   * value supplied by {@code defaultStrSupplier}.
   *
   * <p>Caller responsible for thread-safety and exception handling of default value supplier
   *
   * <pre>{@code
   * StringUtils.getIfEmpty(null, () -> "NULL")    = "NULL"
   * StringUtils.getIfEmpty("", () -> "NULL")      = "NULL"
   * StringUtils.getIfEmpty(" ", () -> "NULL")     = " "
   * StringUtils.getIfEmpty("bat", () -> "NULL")   = "bat"
   * StringUtils.getIfEmpty("", () -> null)        = null
   * StringUtils.getIfEmpty("", null)              = null
   * }</pre>
   *
   * @param <T> the specific kind of CharSequence
   * @param str the CharSequence to check, may be null
   * @param defaultSupplier the supplier of default CharSequence to return if the input is empty
   *     ("") or {@code null}, may be null
   * @return the passed in CharSequence, or the default
   * @see StringUtils#defaultString(String, String)
   * @since 3.10
   */
  public static <T extends CharSequence> T getIfEmpty(
      final T str, final Supplier<T> defaultSupplier) {
    return isEmpty(str) ? Suppliers.get(defaultSupplier) : str;
  }

  public static boolean isNotEmpty(CharSequence cs) {
    return !isEmpty(cs);
  }

  /**
   * Joins the elements of the provided {@link Iterable} into a single String containing the
   * provided elements.
   *
   * <p>No delimiter is added before or after the list. A {@code null} separator is the same as an
   * empty String ("").
   *
   * <p>See the examples here: {@link #join(Object[],String)}.
   *
   * @param iterable the {@link Iterable} providing the values to join together, may be null
   * @param separator the separator character to use, null treated as ""
   * @return the joined String, {@code null} if null iterator input
   * @since 2.3
   */
  public static String join(final Iterable<?> iterable, final String separator) {
    return iterable != null ? join(iterable.iterator(), separator) : null;
  }

  /**
   * Joins the elements of the provided {@link Iterator} into a single String containing the
   * provided elements.
   *
   * <p>No delimiter is added before or after the list. A {@code null} separator is the same as an
   * empty String ("").
   *
   * <p>See the examples here: {@link #join(Object[],String)}.
   *
   * @param iterator the {@link Iterator} of values to join together, may be null
   * @param separator the separator character to use, null treated as ""
   * @return the joined String, {@code null} if null iterator input
   */
  public static String join(final Iterator<?> iterator, final String separator) {
    // handle null, zero and one elements before building a buffer
    if (iterator == null) {
      return null;
    }
    if (!iterator.hasNext()) {
      return EMPTY;
    }
    return Streams.of(iterator)
        .collect(
            LangCollectors.joining(
                toStringOrEmpty(separator), EMPTY, EMPTY, StringUtils::toStringOrEmpty));
  }

  private static String toStringOrEmpty(final Object obj) {
    return Objects.toString(obj, EMPTY);
  }

  /**
   * Joins the elements of the provided array into a single String containing the provided list of
   * elements.
   *
   * <p>No delimiter is added before or after the list. A {@code null} separator is the same as an
   * empty String (""). Null objects or empty strings within the array are represented by empty
   * strings.
   *
   * <pre>
   * StringUtils.join(null, *)                = null
   * StringUtils.join([], *)                  = ""
   * StringUtils.join([null], *)              = ""
   * StringUtils.join(["a", "b", "c"], "--")  = "a--b--c"
   * StringUtils.join(["a", "b", "c"], null)  = "abc"
   * StringUtils.join(["a", "b", "c"], "")    = "abc"
   * StringUtils.join([null, "", "a"], ',')   = ",,a"
   * </pre>
   *
   * @param array the array of values to join together, may be null
   * @param delimiter the separator character to use, null treated as ""
   * @return the joined String, {@code null} if null array input
   */
  public static String join(final Object[] array, final String delimiter) {
    return array != null ? join(array, ObjectUtils.toString(delimiter), 0, array.length) : null;
  }

  /**
   * Joins the elements of the provided array into a single String containing the provided list of
   * elements.
   *
   * <p>No delimiter is added before or after the list. A {@code null} separator is the same as an
   * empty String (""). Null objects or empty strings within the array are represented by empty
   * strings.
   *
   * <pre>
   * StringUtils.join(null, *, *, *)                = null
   * StringUtils.join([], *, *, *)                  = ""
   * StringUtils.join([null], *, *, *)              = ""
   * StringUtils.join(["a", "b", "c"], "--", 0, 3)  = "a--b--c"
   * StringUtils.join(["a", "b", "c"], "--", 1, 3)  = "b--c"
   * StringUtils.join(["a", "b", "c"], "--", 2, 3)  = "c"
   * StringUtils.join(["a", "b", "c"], "--", 2, 2)  = ""
   * StringUtils.join(["a", "b", "c"], null, 0, 3)  = "abc"
   * StringUtils.join(["a", "b", "c"], "", 0, 3)    = "abc"
   * StringUtils.join([null, "", "a"], ',', 0, 3)   = ",,a"
   * </pre>
   *
   * @param array the array of values to join together, may be null
   * @param delimiter the separator character to use, null treated as ""
   * @param startIndex the first index to start joining from.
   * @param endIndex the index to stop joining from (exclusive).
   * @return the joined String, {@code null} if null array input; or the empty string if {@code
   *     endIndex - startIndex <= 0}. The number of joined entries is given by {@code endIndex -
   *     startIndex}
   * @throws ArrayIndexOutOfBoundsException ife<br>
   *     {@code startIndex < 0} or <br>
   *     {@code startIndex >= array.length()} or <br>
   *     {@code endIndex < 0} or <br>
   *     {@code endIndex > array.length()}
   */
  public static String join(
      final Object[] array, final String delimiter, final int startIndex, final int endIndex) {
    return array != null
        ? Streams.of(array)
            .skip(startIndex)
            .limit(Math.max(0, endIndex - startIndex))
            .collect(LangCollectors.joining(delimiter, EMPTY, EMPTY, ObjectUtils::toString))
        : null;
  }

  /**
   * Joins the elements of the provided array into a single String containing the provided list of
   * elements.
   *
   * <p>No delimiter is added before or after the list. Null objects or empty strings within the
   * array are represented by empty strings.
   *
   * <pre>
   * StringUtils.join(null, *)               = null
   * StringUtils.join([], *)                 = ""
   * StringUtils.join([null], *)             = ""
   * StringUtils.join(["a", "b", "c"], ';')  = "a;b;c"
   * StringUtils.join(["a", "b", "c"], null) = "abc"
   * StringUtils.join([null, "", "a"], ';')  = ";;a"
   * </pre>
   *
   * @param array the array of values to join together, may be null
   * @param delimiter the separator character to use
   * @return the joined String, {@code null} if null array input
   * @since 2.0
   */
  public static String join(final Object[] array, final char delimiter) {
    if (array == null) {
      return null;
    }
    return join(array, delimiter, 0, array.length);
  }

  /**
   * Joins the elements of the provided array into a single String containing the provided list of
   * elements.
   *
   * <p>No delimiter is added before or after the list. Null objects or empty strings within the
   * array are represented by empty strings.
   *
   * <pre>
   * StringUtils.join(null, *)               = null
   * StringUtils.join([], *)                 = ""
   * StringUtils.join([null], *)             = ""
   * StringUtils.join(["a", "b", "c"], ';')  = "a;b;c"
   * StringUtils.join(["a", "b", "c"], null) = "abc"
   * StringUtils.join([null, "", "a"], ';')  = ";;a"
   * </pre>
   *
   * @param array the array of values to join together, may be null
   * @param delimiter the separator character to use
   * @param startIndex the first index to start joining from. It is an error to pass in a start
   *     index past the end of the array
   * @param endIndex the index to stop joining from (exclusive). It is an error to pass in an end
   *     index past the end of the array
   * @return the joined String, {@code null} if null array input
   * @since 2.0
   */
  public static String join(
      final Object[] array, final char delimiter, final int startIndex, final int endIndex) {
    return join(array, String.valueOf(delimiter), startIndex, endIndex);
  }
}
