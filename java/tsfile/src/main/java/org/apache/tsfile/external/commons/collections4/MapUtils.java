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

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

import java.util.Map;

public class MapUtils {
  /**
   * Null-safe check if the specified map is empty.
   *
   * <p>Null returns true.
   *
   * @param map the map to check, may be null
   * @return true if empty or null
   * @since 3.2
   */
  public static boolean isEmpty(final Map<?, ?> map) {
    return map == null || map.isEmpty();
  }
}
