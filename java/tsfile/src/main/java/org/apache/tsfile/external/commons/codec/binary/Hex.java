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

package org.apache.tsfile.external.commons.codec.binary;

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

public class Hex {

  /** Used to build output as hex. */
  private static final char[] DIGITS_LOWER = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  /** Used to build output as hex. */
  private static final char[] DIGITS_UPPER = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
  };

  /**
   * Converts an array of bytes into a String representing the hexadecimal values of each byte in
   * order. The returned String will be double the length of the passed array, as it takes two
   * characters to represent any given byte.
   *
   * @param data a byte[] to convert to hexadecimal characters
   * @return A String containing lower-case hexadecimal characters
   * @since 1.4
   */
  public static String encodeHexString(final byte[] data) {
    return new String(encodeHex(data));
  }

  /**
   * Converts an array of bytes into an array of characters representing the hexadecimal values of
   * each byte in order. The returned array will be double the length of the passed array, as it
   * takes two characters to represent any given byte.
   *
   * @param data a byte[] to convert to hexadecimal characters
   * @return A char[] containing lower-case hexadecimal characters
   */
  public static char[] encodeHex(final byte[] data) {
    return encodeHex(data, true);
  }

  /**
   * Converts an array of bytes into an array of characters representing the hexadecimal values of
   * each byte in order. The returned array will be double the length of the passed array, as it
   * takes two characters to represent any given byte.
   *
   * @param data a byte[] to convert to Hex characters
   * @param toLowerCase {@code true} converts to lowercase, {@code false} to uppercase
   * @return A char[] containing hexadecimal characters in the selected case
   * @since 1.4
   */
  public static char[] encodeHex(final byte[] data, final boolean toLowerCase) {
    return encodeHex(data, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
  }

  /**
   * Converts an array of bytes into an array of characters representing the hexadecimal values of
   * each byte in order. The returned array will be double the length of the passed array, as it
   * takes two characters to represent any given byte.
   *
   * @param data a byte[] to convert to hexadecimal characters
   * @param toDigits the output alphabet (must contain at least 16 chars)
   * @return A char[] containing the appropriate characters from the alphabet For best results, this
   *     should be either upper- or lower-case hex.
   * @since 1.4
   */
  protected static char[] encodeHex(final byte[] data, final char[] toDigits) {
    final int dataLength = data.length;
    final char[] out = new char[dataLength << 1];
    encodeHex(data, 0, dataLength, toDigits, out, 0);
    return out;
  }

  /**
   * Converts an array of bytes into an array of characters representing the hexadecimal values of
   * each byte in order.
   *
   * @param data a byte[] to convert to hexadecimal characters
   * @param dataOffset the position in {@code data} to start encoding from
   * @param dataLen the number of bytes from {@code dataOffset} to encode
   * @param toDigits the output alphabet (must contain at least 16 chars)
   * @param out a char[] which will hold the resultant appropriate characters from the alphabet.
   * @param outOffset the position within {@code out} at which to start writing the encoded
   *     characters.
   */
  private static void encodeHex(
      final byte[] data,
      final int dataOffset,
      final int dataLen,
      final char[] toDigits,
      final char[] out,
      final int outOffset) {
    // two characters form the hex value.
    for (int i = dataOffset, j = outOffset; i < dataOffset + dataLen; i++) {
      out[j++] = toDigits[(0xF0 & data[i]) >>> 4];
      out[j++] = toDigits[0x0F & data[i]];
    }
  }
}
