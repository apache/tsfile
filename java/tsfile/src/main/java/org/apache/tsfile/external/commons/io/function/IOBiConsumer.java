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

package org.apache.tsfile.external.commons.io.function;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiConsumer;

/**
 * Like {@link BiConsumer} but throws {@link IOException}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @see BiConsumer
 * @since 2.12.0
 */
@FunctionalInterface
public interface IOBiConsumer<T, U> {

  /**
   * Performs this operation on the given arguments.
   *
   * @param t the first input argument
   * @param u the second input argument
   * @throws IOException if an I/O error occurs.
   */
  void accept(T t, U u) throws IOException;

  /**
   * Creates a {@link BiConsumer} for this instance that throws {@link UncheckedIOException} instead
   * of {@link IOException}.
   *
   * @return an UncheckedIOException BiConsumer.
   * @throws UncheckedIOException Wraps an {@link IOException}.
   * @since 2.12.0
   */
  default BiConsumer<T, U> asBiConsumer() {
    return (t, u) -> Uncheck.accept(this, t, u);
  }
}
