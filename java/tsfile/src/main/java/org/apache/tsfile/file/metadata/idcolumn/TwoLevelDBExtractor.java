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

package org.apache.tsfile.file.metadata.idcolumn;

import org.apache.tsfile.file.metadata.IDeviceID;

/**
 * used for DeviceId whose db level is 2, like root.db There exist three cases for two-level-db
 * tree-style DeviceId: 1. db is DeviceId, like root.db whose DeviceId is {root, db}, no id column
 * in such case 2. device has only one level excluding the db level, like root.db.d1 whose DeviceId
 * is {root.db, d1}, only one id column in segment 1 3. device has more than one level excluding the
 * db level, like root.db.a.d1 whose DeviceId is {root.db.a, d1}, id column start from segment 0
 */
public class TwoLevelDBExtractor implements IDeviceID.TreeDeviceIdColumnValueExtractor {

  // like root.db, treeDBLength will be 7
  private final int treeDBLength;

  public TwoLevelDBExtractor(int treeDBLength) {
    this.treeDBLength = treeDBLength;
  }

  @Override
  public Object extract(IDeviceID treeDeviceId, int idColumnIndex) {
    String firstSegment = (String) treeDeviceId.segment(0);
    int firstSegmentLength = firstSegment.length();
    if (firstSegmentLength < treeDBLength) {
      // case 1
      return null;
    } else if (firstSegmentLength == treeDBLength) {
      // case 2
      return idColumnIndex == 0 ? treeDeviceId.segment(1) : null;
    } else {
      // case 3
      return idColumnIndex == 0
          ? firstSegment.substring(treeDBLength + 1)
          : treeDeviceId.segment(idColumnIndex);
    }
  }
}
