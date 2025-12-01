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

public class ClassUtils {
  /**
   * Delegates to {@link Class#getComponentType()} using generics.
   *
   * @param <T> The array class type.
   * @param cls A class or null.
   * @return The array component type or null.
   * @see Class#getComponentType()
   * @since 3.13.0
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> getComponentType(final Class<T[]> cls) {
    return cls == null ? null : (Class<T>) cls.getComponentType();
  }
}
