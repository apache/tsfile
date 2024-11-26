/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

package org.apache.tsfile.read.expression;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface ExpressionTree {
  boolean satisfy(Object value);

  Filter toFilter();

  class TimeBetweenAnd implements ExpressionTree {
    private long startTime;
    private long endTime;

    public TimeBetweenAnd(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public boolean satisfy(Object value) {
      long v = (Long) value;
      return v >= startTime && v <= endTime;
    }

    @Override
    public Filter toFilter() {
      return TimeFilterApi.between(startTime, endTime);
    }
  }

  class IdColumnMatch implements ExpressionTree {
    private Set<IDeviceID> satisfiedDeviceIds;

    public IdColumnMatch(List<IDeviceID> satisfiedDeviceIdList) {
      this.satisfiedDeviceIds =
          satisfiedDeviceIdList == null ? null : new HashSet<>(satisfiedDeviceIdList);
    }

    @Override
    public boolean satisfy(Object value) {
      return satisfiedDeviceIds == null
          || satisfiedDeviceIds.isEmpty()
          || satisfiedDeviceIds.contains(value);
    }

    @Override
    public Filter toFilter() {
      return null;
    }
  }
}
