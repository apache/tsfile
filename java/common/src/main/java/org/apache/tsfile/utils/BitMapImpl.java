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

package org.apache.tsfile.utils;

abstract class BitMapImpl {

  protected int size;

  protected BitMapImpl(int size) {
    this.size = size;
  }

  int getSize() {
    return size;
  }

  abstract byte[] getByteArray();

  abstract boolean isMarked(int position);

  abstract void markAll();

  abstract void mark(int position);

  abstract void markRange(int startPosition, int length);

  abstract void reset();

  abstract void unmark(int position);

  abstract void unmarkRange(int startPosition, int length);

  abstract void merge(BitMapImpl src, int srcStart, int destStart, int len);

  abstract long extractBits(int offset, int length);

  abstract boolean isAllUnmarked();

  abstract boolean isAllUnmarked(int rangeSize);

  abstract boolean isAllMarked();

  abstract BitMapImpl copy();

  abstract BitMapImpl extend(int newSize);

  abstract long getRetainedSizeInBytes();
}
