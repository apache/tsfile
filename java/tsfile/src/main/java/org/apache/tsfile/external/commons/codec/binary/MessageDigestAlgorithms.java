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

import java.security.MessageDigest;

/**
 * Standard {@link MessageDigest} algorithm names from the <cite>Java Cryptography Architecture
 * Standard Algorithm Name Documentation</cite>.
 *
 * <p>This class is immutable and thread-safe.
 *
 * <ul>
 *   <li>Java 8 and up: SHA-224.
 *   <li>Java 9 and up: SHA3-224, SHA3-256, SHA3-384, SHA3-512.
 * </ul>
 *
 * @see <a
 *     href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#MessageDigest">
 *     Java 8 Cryptography Architecture Standard Algorithm Name Documentation</a>
 * @see <a
 *     href="https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#messagedigest-algorithms">
 *     Java 11 Cryptography Architecture Standard Algorithm Name Documentation</a>
 * @see <a
 *     href="https://docs.oracle.com/en/java/javase/17/docs/specs/security/standard-names.html#messagedigest-algorithms">
 *     Java 17 Cryptography Architecture Standard Algorithm Name Documentation</a>
 * @see <a
 *     href="https://docs.oracle.com/en/java/javase/21/docs/specs/security/standard-names.html#messagedigest-algorithms">
 *     Java 21 Cryptography Architecture Standard Algorithm Name Documentation</a>
 * @see <a href="https://dx.doi.org/10.6028/NIST.FIPS.180-4">FIPS PUB 180-4</a>
 * @see <a href="https://dx.doi.org/10.6028/NIST.FIPS.202">FIPS PUB 202</a>
 * @since 1.7
 */
public class MessageDigestAlgorithms {
  /** The MD2 message digest algorithm defined in RFC 1319. */
  public static final String MD2 = "MD2";
}
