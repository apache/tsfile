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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

public class StringUtils {
  /**
   * Encodes the given string into a sequence of bytes using the UTF-8 charset, storing the result
   * into a new byte array.
   *
   * @param string the String to encode, may be {@code null}
   * @return encoded bytes, or {@code null} if the input string was {@code null}
   * @throws NullPointerException Thrown if {@link StandardCharsets#UTF_8} is not initialized, which
   *     should never happen since it is required by the Java platform specification.
   * @since As of 1.7, throws {@link NullPointerException} instead of UnsupportedEncodingException
   * @see Charset
   * @see #getBytesUnchecked(String, String)
   */
  public static byte[] getBytesUtf8(final String string) {
    return getBytes(string, StandardCharsets.UTF_8);
  }

  /**
   * Calls {@link String#getBytes(Charset)}
   *
   * @param string The string to encode (if null, return null).
   * @param charset The {@link Charset} to encode the {@code String}
   * @return the encoded bytes
   */
  private static byte[] getBytes(final String string, final Charset charset) {
    return string == null ? null : string.getBytes(charset);
  }
}
