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
 * used for DeviceId whose db level is 3, like root.a.db There exist two cases for three-level-db
 * tree-style DeviceId: 1. db is DeviceId, like root.a.db whose DeviceId is {root.a, db}, no id
 * column in such case 2. device has more than zero level excluding the db level, like root.a.db.d1
 * whose DeviceId is {root.a.db, d1}, id column start from segment 1
 */
public class ThreeLevelDBExtractor implements IDeviceID.TreeDeviceIdColumnValueExtractor {

  // like root.a.db, treeDBLength will be 9
  private final int treeDBLength;

  public ThreeLevelDBExtractor(int treeDBLength) {
    this.treeDBLength = treeDBLength;
  }

  @Override
  public Object extract(IDeviceID treeDeviceId, int idColumnIndex) {
    int firstSegmentLength = ((String) treeDeviceId.segment(0)).length();
    if (firstSegmentLength < treeDBLength) {
      // case 1
      return null;
    } else {
      // case 2
      return treeDeviceId.segment(idColumnIndex + 1);
    }
  }
}
