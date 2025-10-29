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

package org.apache.tsfile.external.commons.io;

import java.io.File;

public class FilenameUtils {
  /**
   * Gets the base name, minus the full path and extension, from a full fileName.
   *
   * <p>This method will handle a file in either Unix or Windows format. The text after the last
   * forward or backslash and before the last dot is returned.
   *
   * <pre>
   * a/b/c.txt --&gt; c
   * a.txt     --&gt; a
   * a/b/c     --&gt; c
   * a/b/c/    --&gt; ""
   * </pre>
   *
   * <p>The output will be the same irrespective of the machine that the code is running on.
   *
   * @param fileName the fileName to query, null returns null
   * @return the name of the file without the path, or an empty string if none exists
   * @throws IllegalArgumentException if the fileName contains the null character ({@code U+0000})
   */
  public static String getBaseName(final String fileName) {
    return removeExtension(getName(fileName));
  }

  /**
   * Removes the extension from a fileName.
   *
   * <p>This method returns the textual part of the fileName before the last dot. There must be no
   * directory separator after the dot.
   *
   * <pre>
   * foo.txt    --&gt; foo
   * a\b\c.jpg  --&gt; a\b\c
   * a\b\c      --&gt; a\b\c
   * a.b\c      --&gt; a.b\c
   * </pre>
   *
   * <p>The output will be the same irrespective of the machine that the code is running on.
   *
   * @param fileName the fileName to query, null returns null
   * @return the fileName minus the extension
   * @throws IllegalArgumentException if the fileName contains the null character ({@code U+0000})
   */
  public static String removeExtension(final String fileName) {
    if (fileName == null) {
      return null;
    }
    requireNonNullChars(fileName);

    final int index = indexOfExtension(fileName);
    if (index == NOT_FOUND) {
      return fileName;
    }
    return fileName.substring(0, index);
  }

  /**
   * Gets the name minus the path from a full fileName.
   *
   * <p>This method will handle a file in either Unix or Windows format. The text after the last
   * forward or backslash is returned.
   *
   * <pre>
   * a/b/c.txt --&gt; c.txt
   * a.txt     --&gt; a.txt
   * a/b/c     --&gt; c
   * a/b/c/    --&gt; ""
   * </pre>
   *
   * <p>The output will be the same irrespective of the machine that the code is running on.
   *
   * @param fileName the fileName to query, null returns null
   * @return the name of the file without the path, or an empty string if none exists
   * @throws IllegalArgumentException if the fileName contains the null character ({@code U+0000})
   */
  public static String getName(final String fileName) {
    if (fileName == null) {
      return null;
    }
    return requireNonNullChars(fileName).substring(indexOfLastSeparator(fileName) + 1);
  }

  /**
   * Checks the input for null characters ({@code U+0000}), a sign of unsanitized data being passed
   * to file level functions.
   *
   * <p>This may be used for poison byte attacks.
   *
   * @param path the path to check
   * @return The input
   * @throws IllegalArgumentException if path contains the null character ({@code U+0000})
   */
  private static String requireNonNullChars(final String path) {
    if (path.indexOf(0) >= 0) {
      throw new IllegalArgumentException(
          "Null character present in file/path name. There are no known legitimate use cases for such data, but several injection attacks may use it");
    }
    return path;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static final int NOT_FOUND = -1;
  private static final String EMPTY_STRING = "";

  /**
   * The extension separator character.
   *
   * @since 1.4
   */
  public static final char EXTENSION_SEPARATOR = '.';

  /** The system separator character. */
  private static final char SYSTEM_NAME_SEPARATOR = File.separatorChar;

  /** The separator character that is the opposite of the system separator. */
  private static final char OTHER_SEPARATOR = flipSeparator(SYSTEM_NAME_SEPARATOR);

  /** The Unix separator character. */
  private static final char UNIX_NAME_SEPARATOR = '/';

  /** The Windows separator character. */
  private static final char WINDOWS_NAME_SEPARATOR = '\\';

  /**
   * Gets the extension of a fileName.
   *
   * <p>This method returns the textual part of the fileName after the last dot. There must be no
   * directory separator after the dot.
   *
   * <pre>
   * foo.txt      --&gt; "txt"
   * a/b/c.jpg    --&gt; "jpg"
   * a/b.txt/c    --&gt; ""
   * a/b/c        --&gt; ""
   * </pre>
   *
   * <p>The output will be the same irrespective of the machine that the code is running on, with
   * the exception of a possible {@link IllegalArgumentException} on Windows (see below).
   *
   * <p><b>Note:</b> This method used to have a hidden problem for names like "foo.exe:bar.txt". In
   * this case, the name wouldn't be the name of a file, but the identifier of an alternate data
   * stream (bar.txt) on the file foo.exe. The method used to return ".txt" here, which would be
   * misleading. Commons IO 2.7, and later versions, are throwing an {@link
   * IllegalArgumentException} for names like this.
   *
   * @param fileName the fileName to retrieve the extension of.
   * @return the extension of the file or an empty string if none exists or {@code null} if the
   *     fileName is {@code null}.
   * @throws IllegalArgumentException <b>Windows only:</b> The fileName parameter is, in fact, the
   *     identifier of an Alternate Data Stream, for example "foo.exe:bar.txt".
   */
  public static String getExtension(final String fileName) throws IllegalArgumentException {
    if (fileName == null) {
      return null;
    }
    final int index = indexOfExtension(fileName);
    if (index == NOT_FOUND) {
      return EMPTY_STRING;
    }
    return fileName.substring(index + 1);
  }

  /**
   * Returns the index of the last extension separator character, which is a dot.
   *
   * <p>This method also checks that there is no directory separator after the last dot. To do this
   * it uses {@link #indexOfLastSeparator(String)} which will handle a file in either Unix or
   * Windows format.
   *
   * <p>The output will be the same irrespective of the machine that the code is running on, with
   * the exception of a possible {@link IllegalArgumentException} on Windows (see below).
   * <b>Note:</b> This method used to have a hidden problem for names like "foo.exe:bar.txt". In
   * this case, the name wouldn't be the name of a file, but the identifier of an alternate data
   * stream (bar.txt) on the file foo.exe. The method used to return ".txt" here, which would be
   * misleading. Commons IO 2.7, and later versions, are throwing an {@link
   * IllegalArgumentException} for names like this.
   *
   * @param fileName the fileName to find the last extension separator in, null returns -1
   * @return the index of the last extension separator character, or -1 if there is no such
   *     character
   * @throws IllegalArgumentException <b>Windows only:</b> The fileName parameter is, in fact, the
   *     identifier of an Alternate Data Stream, for example "foo.exe:bar.txt".
   */
  public static int indexOfExtension(final String fileName) throws IllegalArgumentException {
    if (fileName == null) {
      return NOT_FOUND;
    }
    if (isSystemWindows()) {
      // Special handling for NTFS ADS: Don't accept colon in the fileName.
      final int offset = fileName.indexOf(':', getAdsCriticalOffset(fileName));
      if (offset != -1) {
        throw new IllegalArgumentException("NTFS ADS separator (':') in file name is forbidden.");
      }
    }
    final int extensionPos = fileName.lastIndexOf(EXTENSION_SEPARATOR);
    final int lastSeparator = indexOfLastSeparator(fileName);
    return lastSeparator > extensionPos ? NOT_FOUND : extensionPos;
  }

  /**
   * Determines if Windows file system is in use.
   *
   * @return true if the system is Windows
   */
  static boolean isSystemWindows() {
    return SYSTEM_NAME_SEPARATOR == WINDOWS_NAME_SEPARATOR;
  }

  /**
   * Flips the Windows name separator to Linux and vice-versa.
   *
   * @param ch The Windows or Linux name separator.
   * @return The Windows or Linux name separator.
   */
  static char flipSeparator(final char ch) {
    if (ch == UNIX_NAME_SEPARATOR) {
      return WINDOWS_NAME_SEPARATOR;
    }
    if (ch == WINDOWS_NAME_SEPARATOR) {
      return UNIX_NAME_SEPARATOR;
    }
    throw new IllegalArgumentException(String.valueOf(ch));
  }

  /**
   * Special handling for NTFS ADS: Don't accept colon in the fileName.
   *
   * @param fileName a file name
   * @return ADS offsets.
   */
  private static int getAdsCriticalOffset(final String fileName) {
    // Step 1: Remove leading path segments.
    final int offset1 = fileName.lastIndexOf(SYSTEM_NAME_SEPARATOR);
    final int offset2 = fileName.lastIndexOf(OTHER_SEPARATOR);
    if (offset1 == -1) {
      if (offset2 == -1) {
        return 0;
      }
      return offset2 + 1;
    }
    if (offset2 == -1) {
      return offset1 + 1;
    }
    return Math.max(offset1, offset2) + 1;
  }

  /**
   * Returns the index of the last directory separator character.
   *
   * <p>This method will handle a file in either Unix or Windows format. The position of the last
   * forward or backslash is returned.
   *
   * <p>The output will be the same irrespective of the machine that the code is running on.
   *
   * @param fileName the fileName to find the last path separator in, null returns -1
   * @return the index of the last separator character, or -1 if there is no such character
   */
  public static int indexOfLastSeparator(final String fileName) {
    if (fileName == null) {
      return NOT_FOUND;
    }
    final int lastUnixPos = fileName.lastIndexOf(UNIX_NAME_SEPARATOR);
    final int lastWindowsPos = fileName.lastIndexOf(WINDOWS_NAME_SEPARATOR);
    return Math.max(lastUnixPos, lastWindowsPos);
  }
}
