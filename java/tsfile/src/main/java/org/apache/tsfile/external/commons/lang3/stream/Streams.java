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

package org.apache.tsfile.external.commons.lang3.stream;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Streams {
  /**
   * Creates a stream on the given Iterator.
   *
   * @param <E> the type of elements in the Iterator.
   * @param iterator the Iterator to stream or null.
   * @return a new Stream or {@link Stream#empty()} if the Iterator is null.
   * @since 3.13.0
   */
  public static <E> Stream<E> of(final Iterator<E> iterator) {
    return iterator == null
        ? Stream.empty()
        : StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }

  /**
   * Null-safe version of {@link Stream#of(Object[])}.
   *
   * @param <T> the type of stream elements.
   * @param values the elements of the new stream, may be {@code null}.
   * @return the new stream on {@code values} or {@link Stream#empty()}.
   * @since 3.13.0
   */
  @SafeVarargs // Creating a stream from an array is safe
  public static <T> Stream<T> of(final T... values) {
    return values == null ? Stream.empty() : Stream.of(values);
  }
}
