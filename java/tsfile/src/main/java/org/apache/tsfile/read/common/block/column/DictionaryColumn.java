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

package org.apache.tsfile.read.common.block.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkArrayRange;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkValidPosition;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.compactArray;
import static org.apache.tsfile.read.common.block.column.DictionaryId.randomDictionaryId;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOf;

public final class DictionaryColumn implements Column {
  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(DictionaryColumn.class)
          + (int) RamUsageEstimator.shallowSizeOfInstance(DictionaryId.class);

  private int positionCount;
  private final Column dictionary;
  private final int idsOffset;
  private final int[] ids;
  private final long retainedSizeInBytes;
  private volatile int uniqueIds = -1;
  // isSequentialIds is only valid when uniqueIds is computed
  private volatile boolean isSequentialIds;
  private final DictionaryId dictionarySourceId;
  private final boolean mayHaveNull;

  public static Column create(int positionCount, Column dictionary, int[] ids) {
    return createInternal(0, positionCount, dictionary, ids, randomDictionaryId());
  }

  /** This should not only be used when creating a projection of another dictionary Column. */
  public static Column createProjectedDictionaryColumn(
      int positionCount, Column dictionary, int[] ids, DictionaryId dictionarySourceId) {
    return createInternal(0, positionCount, dictionary, ids, dictionarySourceId);
  }

  static Column createInternal(
      int idsOffset,
      int positionCount,
      Column dictionary,
      int[] ids,
      DictionaryId dictionarySourceId) {
    if (positionCount == 0) {
      return dictionary.getRegionCopy(0, 0);
    }
    if (positionCount == 1) {
      return dictionary.getRegion(ids[idsOffset], 1);
    }

    // if dictionary is an RLE then this can just be a new RLE
    if (dictionary instanceof RunLengthEncodedColumn) {
      RunLengthEncodedColumn rle = (RunLengthEncodedColumn) dictionary;
      return new RunLengthEncodedColumn(rle.getValue(), positionCount);
    }

    if (dictionary instanceof DictionaryColumn) {
      DictionaryColumn dictionaryColumn = (DictionaryColumn) dictionary;
      // unwrap dictionary in dictionary
      int[] newIds = new int[positionCount];
      for (int position = 0; position < positionCount; position++) {
        newIds[position] = dictionaryColumn.getId(ids[idsOffset + position]);
      }
      return new DictionaryColumn(
          0,
          positionCount,
          dictionaryColumn.getDictionary(),
          newIds,
          false,
          false,
          randomDictionaryId());
    }

    return new DictionaryColumn(
        idsOffset, positionCount, dictionary, ids, false, false, dictionarySourceId);
  }

  DictionaryColumn(
      int idsOffset,
      int positionCount,
      Column dictionary,
      int[] ids,
      boolean dictionaryIsCompacted,
      boolean isSequentialIds,
      DictionaryId dictionarySourceId) {
    requireNonNull(dictionary, "dictionary is null");
    requireNonNull(ids, "ids is null");

    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }

    this.idsOffset = idsOffset;
    if (ids.length - idsOffset < positionCount) {
      throw new IllegalArgumentException("ids length is less than positionCount");
    }

    this.positionCount = positionCount;
    this.dictionary = dictionary;
    this.ids = ids;
    this.dictionarySourceId = requireNonNull(dictionarySourceId, "dictionarySourceId is null");
    this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(ids);
    // avoid eager loading of lazy dictionaries
    this.mayHaveNull = positionCount > 0 && dictionary.mayHaveNull();

    if (dictionaryIsCompacted) {
      this.uniqueIds = dictionary.getPositionCount();
    }

    if (isSequentialIds && !dictionaryIsCompacted) {
      throw new IllegalArgumentException(
          "sequential ids flag is only valid for compacted dictionary");
    }
    this.isSequentialIds = isSequentialIds;
  }

  public int[] getRawIds() {
    return ids;
  }

  public int getRawIdsOffset() {
    return idsOffset;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  private void calculateCompactSize() {
    int uniqueIds = 0;
    boolean[] used = new boolean[dictionary.getPositionCount()];
    // nested dictionaries are assumed not to have sequential ids
    boolean isSequentialIds = true;
    int previousPosition = -1;
    for (int i = 0; i < positionCount; i++) {
      int position = ids[idsOffset + i];
      // Avoid branching
      uniqueIds += used[position] ? 0 : 1;
      used[position] = true;
      if (isSequentialIds) {
        // this branch is predictable and will switch paths at most once while looping
        isSequentialIds = previousPosition < position;
        previousPosition = position;
      }
    }

    this.uniqueIds = uniqueIds;
    this.isSequentialIds = isSequentialIds;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes + dictionary.getRetainedSizeInBytes();
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);

    if (length == positionCount) {
      return this;
    }

    return new DictionaryColumn(
        idsOffset + positionOffset, length, dictionary, ids, false, false, dictionarySourceId);
  }

  @Override
  public Column getRegionCopy(int position, int length) {
    checkValidRegion(positionCount, position, length);
    if (length == 0) {
      // explicit support for case when length == 0 which might otherwise fail
      // on getId(position) if position == positionCount
      return dictionary.getRegionCopy(0, 0);
    }
    // Avoid repeated volatile reads to the uniqueIds field
    int uniqueIds = this.uniqueIds;
    if (length <= 1 || (uniqueIds == dictionary.getPositionCount() && isSequentialIds)) {
      // copy the contiguous range directly via copyRegion
      return dictionary.getRegionCopy(getId(position), length);
    }
    if (uniqueIds == positionCount) {
      // each Column position is unique or the dictionary is a nested dictionary Column,
      // therefore it makes sense to unwrap this outer dictionary layer directly
      return dictionary.copyPositions(ids, idsOffset + position, length);
    }
    int[] newIds = compactArray(ids, idsOffset + position, length);
    if (newIds == ids) {
      return this;
    }
    return new DictionaryColumn(0, length, dictionary, newIds, false, false, randomDictionaryId())
        .compact();
  }

  @Override
  public Column subColumn(int fromIndex) {
    return getRegion(fromIndex, positionCount - fromIndex);
  }

  @Override
  public Column subColumnCopy(int fromIndex) {
    return getRegionCopy(fromIndex, positionCount - fromIndex);
  }

  @Override
  public TSDataType getDataType() {
    return dictionary.getDataType();
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.DICTIONARY;
  }

  @Override
  public boolean mayHaveNull() {
    return mayHaveNull && dictionary.mayHaveNull();
  }

  @Override
  public boolean isNull(int position) {
    if (!mayHaveNull) {
      return false;
    }
    checkValidPosition(position, positionCount);
    return dictionary.isNull(getIdUnchecked(position));
  }

  @Override
  public boolean[] isNull() {
    boolean[] original = dictionary.isNull();
    boolean[] res = new boolean[positionCount];

    if (original == null) {
      Arrays.fill(res, false);
      return res;
    }

    for (int i = 0; i < positionCount; i++) {
      int position = ids[idsOffset + i];
      res[i] = dictionary.isNull(position);
    }
    return res;
  }

  @Override
  public void setNull(int start, int end) {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  @Override
  public Column getPositions(int[] positions, int offset, int length) {
    checkArrayRange(positions, offset, length);

    int[] newIds = new int[length];
    boolean isCompact = length >= dictionary.getPositionCount() && isCompact();
    boolean[] usedIds = isCompact ? new boolean[dictionary.getPositionCount()] : null;
    int uniqueIds = 0;
    for (int i = 0; i < length; i++) {
      int id = getId(positions[offset + i]);
      newIds[i] = id;
      if (usedIds != null) {
        uniqueIds += usedIds[id] ? 0 : 1;
        usedIds[id] = true;
      }
    }
    // All positions must have been referenced in order to be compact
    isCompact &= (usedIds != null && usedIds.length == uniqueIds);
    DictionaryColumn result =
        new DictionaryColumn(
            0, newIds.length, dictionary, newIds, isCompact, false, getDictionarySourceId());
    if (usedIds != null && !isCompact) {
      // resulting dictionary is not compact, but we know the number of unique ids and which
      // positions are used
      result.uniqueIds = uniqueIds;
    }
    return result;
  }

  @Override
  public Column copyPositions(int[] positions, int offset, int length) {
    checkArrayRange(positions, offset, length);

    if (length <= 1 || uniqueIds == positionCount) {
      // each block position is unique or the dictionary is a nested dictionary block,
      // therefore it makes sense to unwrap this outer dictionary layer directly
      int[] positionsToCopy = new int[length];
      for (int i = 0; i < length; i++) {
        positionsToCopy[i] = getId(positions[offset + i]);
      }
      return dictionary.copyPositions(positionsToCopy, 0, length);
    }

    List<Integer> positionsToCopy = new ArrayList<>();
    HashMap<Integer, Integer> oldIndexToNewIndex =
        new HashMap<>(min(length, dictionary.getPositionCount()));
    int[] newIds = new int[length];

    for (int i = 0; i < length; i++) {
      int position = positions[offset + i];
      int oldIndex = getId(position);
      Integer newId = oldIndexToNewIndex.putIfAbsent(oldIndex, positionsToCopy.size());
      if (newId == null) {
        newId = positionsToCopy.size();
        positionsToCopy.add(oldIndex);
      }
      newIds[i] = newId;
    }
    Column compactDictionary =
        dictionary.copyPositions(
            ArrayUtils.toPrimitive(positionsToCopy.toArray(new Integer[0])),
            0,
            positionsToCopy.size());
    if (positionsToCopy.size() == length) {
      // discovered that all positions are unique, so return the unwrapped underlying dictionary
      // directly
      return compactDictionary;
    }
    return new DictionaryColumn(
        0,
        length,
        compactDictionary,
        newIds,
        true, // new dictionary is compact
        false,
        randomDictionaryId());
  }

  @Override
  public void reverse() {
    int currIndex = idsOffset;
    int endIndex = ids.length - 1;
    while (currIndex < endIndex) {
      int temp = ids[currIndex];
      ids[currIndex] = ids[endIndex];
      ids[endIndex] = temp;

      currIndex++;
      endIndex--;
    }
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public void setPositionCount(int count) {
    positionCount = count;
  }

  @Override
  public boolean getBoolean(int position) {
    return dictionary.getBoolean(getId(position));
  }

  @Override
  public int getInt(int position) {
    return dictionary.getInt(getId(position));
  }

  @Override
  public long getLong(int position) {
    return dictionary.getLong(getId(position));
  }

  @Override
  public float getFloat(int position) {
    return dictionary.getFloat(getId(position));
  }

  @Override
  public double getDouble(int position) {
    return dictionary.getDouble(getId(position));
  }

  @Override
  public Binary getBinary(int position) {
    return dictionary.getBinary(getId(position));
  }

  @Override
  public Object getObject(int position) {
    return dictionary.getObject(getId(position));
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    return dictionary.getTsPrimitiveType(position);
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException(getClass().getSimpleName());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DictionaryColumn{");
    sb.append("positionCount=").append(getPositionCount());
    sb.append('}');
    return sb.toString();
  }

  private Column getUnderlyingColumn() {
    return dictionary;
  }

  private int getUnderlyingValuePosition(int position) {
    return getId(position);
  }

  public Column getDictionary() {
    return dictionary;
  }

  public int getId(int position) {
    checkValidPosition(position, positionCount);
    return getIdUnchecked(position);
  }

  private int getIdUnchecked(int position) {
    return ids[position + idsOffset];
  }

  public DictionaryId getDictionarySourceId() {
    return dictionarySourceId;
  }

  public boolean isCompact() {
    if (uniqueIds == -1) {
      calculateCompactSize();
    }
    return uniqueIds == dictionary.getPositionCount();
  }

  public DictionaryColumn compact() {
    if (isCompact()) {
      return this;
    }

    // determine which dictionary entries are referenced and build a reindex for them
    int dictionarySize = dictionary.getPositionCount();
    List<Integer> dictionaryPositionsToCopy = new ArrayList<>(min(dictionarySize, positionCount));
    int[] remapIndex = new int[dictionarySize];
    Arrays.fill(remapIndex, -1);

    int newIndex = 0;
    for (int i = 0; i < positionCount; i++) {
      int dictionaryIndex = getId(i);
      if (remapIndex[dictionaryIndex] == -1) {
        dictionaryPositionsToCopy.add(dictionaryIndex);
        remapIndex[dictionaryIndex] = newIndex;
        newIndex++;
      }
    }

    // entire dictionary is referenced
    if (dictionaryPositionsToCopy.size() == dictionarySize) {
      return this;
    }

    // compact the dictionary
    int[] newIds = new int[positionCount];
    for (int i = 0; i < positionCount; i++) {
      int newId = remapIndex[getId(i)];
      if (newId == -1) {
        throw new IllegalStateException("reference to a non-existent key");
      }
      newIds[i] = newId;
    }
    try {
      Column compactDictionary =
          dictionary.copyPositions(
              ArrayUtils.toPrimitive(dictionaryPositionsToCopy.toArray(new Integer[0])),
              0,
              dictionaryPositionsToCopy.size());
      return new DictionaryColumn(
          0,
          positionCount,
          compactDictionary,
          newIds,
          true,
          // Copied dictionary positions match ids sequence. Therefore new
          // compact dictionary block has sequential ids only if single position
          // is not used more than once.
          uniqueIds == positionCount,
          randomDictionaryId());
    } catch (UnsupportedOperationException e) {
      // ignore if copy positions is not supported for the dictionary block
      return this;
    }
  }

  /**
   * Compact the dictionary down to only the used positions for a set of Columns that have been
   * projected from the same dictionary.
   */
  public static List<DictionaryColumn> compactRelatedColumns(List<DictionaryColumn> Columns) {
    DictionaryColumn firstDictionaryColumn = Columns.get(0);
    Column dictionary = firstDictionaryColumn.getDictionary();

    int positionCount = firstDictionaryColumn.getPositionCount();
    int dictionarySize = dictionary.getPositionCount();

    // determine which dictionary entries are referenced and build a reindex for them
    int[] dictionaryPositionsToCopy = new int[min(dictionarySize, positionCount)];
    int[] remapIndex = new int[dictionarySize];
    Arrays.fill(remapIndex, -1);

    int numberOfIndexes = 0;
    for (int i = 0; i < positionCount; i++) {
      int position = firstDictionaryColumn.getId(i);
      if (remapIndex[position] == -1) {
        dictionaryPositionsToCopy[numberOfIndexes] = position;
        remapIndex[position] = numberOfIndexes;
        numberOfIndexes++;
      }
    }

    // entire dictionary is referenced
    if (numberOfIndexes == dictionarySize) {
      return Columns;
    }

    // compact the dictionaries
    int[] newIds = getNewIds(positionCount, firstDictionaryColumn, remapIndex);
    List<DictionaryColumn> outputDictionaryColumns = new ArrayList<>(Columns.size());
    DictionaryId newDictionaryId = randomDictionaryId();
    for (DictionaryColumn dictionaryColumn : Columns) {
      if (!firstDictionaryColumn
          .getDictionarySourceId()
          .equals(dictionaryColumn.getDictionarySourceId())) {
        throw new IllegalArgumentException("dictionarySourceIds must be the same");
      }

      try {
        Column compactDictionary =
            dictionaryColumn
                .getDictionary()
                .copyPositions(dictionaryPositionsToCopy, 0, numberOfIndexes);
        outputDictionaryColumns.add(
            new DictionaryColumn(
                0, positionCount, compactDictionary, newIds, true, false, newDictionaryId));
      } catch (UnsupportedOperationException e) {
        // ignore if copy positions is not supported for the dictionary
        outputDictionaryColumns.add(dictionaryColumn);
      }
    }
    return outputDictionaryColumns;
  }

  private static int[] getNewIds(
      int positionCount, DictionaryColumn dictionaryColumn, int[] remapIndex) {
    int[] newIds = new int[positionCount];
    for (int i = 0; i < positionCount; i++) {
      int newId = remapIndex[dictionaryColumn.getId(i)];
      if (newId == -1) {
        throw new IllegalStateException("reference to a non-existent key");
      }
      newIds[i] = newId;
    }
    return newIds;
  }
}
