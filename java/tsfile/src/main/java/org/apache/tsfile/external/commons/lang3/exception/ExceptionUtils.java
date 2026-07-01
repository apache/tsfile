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

package org.apache.tsfile.external.commons.lang3.exception;

import java.util.ArrayList;
import java.util.List;

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

public class ExceptionUtils {

  /**
   * Walks the {@link Throwable} to obtain its root cause.
   *
   * <p>This method walks through the exception chain until the last element, the root cause of the
   * chain, using {@link Throwable#getCause()}, and returns that exception.
   *
   * <p>This method handles recursive cause chains that might otherwise cause infinite loops. The
   * cause chain is processed until the end, or until the next item in the chain is already
   * processed. If we detect a loop, then return the element before the loop.
   *
   * @param throwable the throwable to get the root cause for, may be null
   * @return the root cause of the {@link Throwable}, {@code null} if null throwable input
   */
  public static Throwable getRootCause(final Throwable throwable) {
    final List<Throwable> list = getThrowableList(throwable);
    return list.isEmpty() ? null : list.get(list.size() - 1);
  }

  /**
   * Gets the list of {@link Throwable} objects in the exception chain.
   *
   * <p>A throwable without cause will return a list containing one element - the input throwable. A
   * throwable with one cause will return a list containing two elements. - the input throwable and
   * the cause throwable. A {@code null} throwable will return a list of size zero.
   *
   * <p>This method handles recursive cause chains that might otherwise cause infinite loops. The
   * cause chain is processed until the end, or until the next item in the chain is already in the
   * result list.
   *
   * @param throwable the throwable to inspect, may be null
   * @return the list of throwables, never null
   * @since 2.2
   */
  public static List<Throwable> getThrowableList(Throwable throwable) {
    final List<Throwable> list = new ArrayList<>();
    while (throwable != null && !list.contains(throwable)) {
      list.add(throwable);
      throwable = throwable.getCause();
    }
    return list;
  }
}
