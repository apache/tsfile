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

package org.apache.tsfile.external.commons.collections4;

import java.util.Collection;

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

public class CollectionUtils {
  /**
   * Null-safe check if the specified collection is empty.
   *
   * <p>Null returns true.
   *
   * @param coll the collection to check, may be null
   * @return true if empty or null
   * @since 3.2
   */
  public static boolean isEmpty(final Collection<?> coll) {
    return coll == null || coll.isEmpty();
  }

  /**
   * Null-safe check if the specified collection is not empty.
   *
   * <p>Null returns false.
   *
   * @param coll the collection to check, may be null
   * @return true if non-null and non-empty
   * @since 3.2
   */
  public static boolean isNotEmpty(final Collection<?> coll) {
    return !isEmpty(coll);
  }
}
