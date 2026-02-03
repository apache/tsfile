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

namespace Apache.TsFile.Encoding.BitPacking;

public class IntPacker
{
    private const int NumOfInts = 8;
    private int _width;

    public IntPacker(int width)
    {
        _width = width;
    }

    public void Pack8Values(int[] values, int offset, byte[] buf)
    {
        int bufIdx = 0;
        int valueIdx = offset;
        int leftBit = 0;

        while (valueIdx < NumOfInts + offset)
        {
            int buffer = 0;
            int leftSize = 32;

            if (leftBit > 0)
            {
                buffer |= (values[valueIdx] << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= _width && valueIdx < NumOfInts + offset)
            {
                buffer |= (values[valueIdx] << (leftSize - _width));
                leftSize -= _width;
                valueIdx++;
            }

            if (leftSize > 0 && valueIdx < NumOfInts + offset)
            {
                buffer |= (int)((uint)values[valueIdx] >> (_width - leftSize));
                leftBit = _width - leftSize;
            }

            for (int j = 0; j < 4; j++)
            {
                buf[bufIdx] = (byte)((buffer >> ((3 - j) * 8)) & 0xFF);
                bufIdx++;
                if (bufIdx >= _width)
                {
                    return;
                }
            }
        }
    }

    public void Unpack8Values(byte[] buf, int offset, int[] values)
    {
        int byteIdx = offset;
        long buffer = 0;
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < NumOfInts)
        {
            while (totalBits < _width)
            {
                buffer = (buffer << 8) | (uint)(buf[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            while (totalBits >= _width && valueIdx < 8)
            {
                values[valueIdx] = (int)(buffer >> (totalBits - _width));
                valueIdx++;
                totalBits -= _width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public void SetWidth(int width)
    {
        _width = width;
    }
}
