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

public class LongPacker
{
    private const int NumOfLongs = 8;
    private int _width;

    public LongPacker(int width)
    {
        _width = width;
    }

    public void Pack8Values(long[] values, int offset, byte[] buf)
    {
        int bufIdx = 0;
        int valueIdx = offset;
        int leftBit = 0;

        while (valueIdx < NumOfLongs + offset)
        {
            long buffer = 0;
            int leftSize = 64;

            if (leftBit > 0)
            {
                buffer |= (values[valueIdx] << (64 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= _width && valueIdx < NumOfLongs + offset)
            {
                buffer |= (values[valueIdx] << (leftSize - _width));
                leftSize -= _width;
                valueIdx++;
            }

            if (leftSize > 0 && valueIdx < NumOfLongs + offset)
            {
                buffer |= (long)((ulong)values[valueIdx] >> (_width - leftSize));
                leftBit = _width - leftSize;
            }

            for (int j = 0; j < 8; j++)
            {
                buf[bufIdx] = (byte)((buffer >> ((8 - j - 1) * 8)) & 0xFF);
                bufIdx++;
                if (bufIdx >= _width * 8 / 8)
                {
                    return;
                }
            }
        }
    }

    public void Unpack8Values(byte[] buf, int offset, long[] values)
    {
        int byteIdx = offset;
        int valueIdx = 0;
        int leftBits = 8;
        int totalBits = 0;

        while (valueIdx < 8)
        {
            values[valueIdx] = 0;
            while (totalBits < _width)
            {
                if (_width - totalBits >= leftBits)
                {
                    values[valueIdx] = values[valueIdx] << leftBits;
                    values[valueIdx] = values[valueIdx] | (long)(((1L << leftBits) - 1) & buf[byteIdx]);
                    totalBits += leftBits;
                    byteIdx++;
                    leftBits = 8;
                }
                else
                {
                    int t = _width - totalBits;
                    values[valueIdx] = values[valueIdx] << t;
                    values[valueIdx] = values[valueIdx] | (long)((((1L << leftBits) - 1) & buf[byteIdx]) >> (leftBits - t));
                    leftBits -= t;
                    totalBits += t;
                }
            }
            valueIdx++;
            totalBits = 0;
        }
    }

    public void SetWidth(int width)
    {
        _width = width;
    }
}
