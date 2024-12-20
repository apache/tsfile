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

import static org.apache.tsfile.utils.Preconditions.checkArgument;

/**
 * used for DeviceId whose db level is 4 or higher, like root.a.b.db There only exist one case for
 * four-or-higher-level-db tree-style DeviceId: 1. first segment will be the first three level of
 * db, so id column index starting from db_level - 2 like root.a.b.db whose DeviceId is {root.a.b,
 * db}, no id column in such case, also call say that id column starting from 2(not existing) like
 * root.a.b.db.d1 whose DeviceId is {root.a.b, db, d1}, id column starting from 2 like
 * root.a.b.db.c.d1 whose DeviceId is {root.a.b, db, c, d1}, id column starting from 3
 */
public class FourOrHigherLevelDBExtractor implements IDeviceID.TreeDeviceIdColumnValueExtractor {

  private final int idColumnStartIndex;

  public FourOrHigherLevelDBExtractor(int dbLevelNumber) {
    checkArgument(dbLevelNumber >= 4, "db level number must be >= 4");
    this.idColumnStartIndex = dbLevelNumber - 2;
  }

  @Override
  public Object extract(IDeviceID treeDeviceId, int idColumnIndex) {
    return treeDeviceId.segment(idColumnStartIndex + idColumnIndex);
  }
}
