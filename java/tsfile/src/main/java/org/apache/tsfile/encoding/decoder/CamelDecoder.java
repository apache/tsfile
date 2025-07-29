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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.common.bitStream.BitInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

public class CamelDecoder {
    GorillaDecoder gorillaDecoder;

    private long previousValue = 0;

    private boolean isFirst = true;

    public class GorillaDecoder {
        private int leadingZeros = Integer.MAX_VALUE;
        private int trailingZeros = 0;

        public boolean hasNext(BitInputStream in) throws IOException {
            return in.availableBits() >= 0;
        }

        public double decode(BitInputStream in) throws IOException {
            if (isFirst) {
                previousValue = in.readLong(64);
                isFirst = false;
                return Double.longBitsToDouble(previousValue);
            }

            if (!in.readBit()) {
                return Double.longBitsToDouble(previousValue); // same value
            }

            boolean hasNewControl = in.readBit();
            long xor;
            if (!hasNewControl) {
                int significantBits = 64 - leadingZeros - trailingZeros;
                if (significantBits == 0) {
                    return Double.longBitsToDouble(previousValue); // no change
                }
                xor = in.readLong(significantBits) << trailingZeros;
            } else {
                leadingZeros = in.readInt(6);
                int significantBits = in.readInt(6) + 1;
                trailingZeros = 64 - leadingZeros - significantBits;
                xor = in.readLong(significantBits) << trailingZeros;
            }

            previousValue ^= xor;
            return Double.longBitsToDouble(previousValue);
        }
    }


    private final BitInputStream in;

    private long storedVal = 0;

    private final static int DECIMAL_MAX_COUNT = 15;
    public static final long[] powers = new long[DECIMAL_MAX_COUNT];

    // threshold[l-1] = 10^l / 2^l
    public static final long[] threshold = new long[DECIMAL_MAX_COUNT];

    // mValueBits[l-1] = ceil(log2(threshold[l-1]))
    public static final int[] mValueBits = new int[DECIMAL_MAX_COUNT];

    public CamelDecoder(InputStream in, long totalBits) {
        for (int l = 1; l <= DECIMAL_MAX_COUNT; l++) {
            int idx = l - 1;
            powers[idx] = (long) Math.pow(10, l);
            long divisor = 1L << l;  // 2^l
            threshold[idx] = powers[idx] / divisor;
            mValueBits[idx] = (int) Math.ceil(Math.log(threshold[idx]) / Math.log(2));
        }
        this.in = new BitInputStream(in, totalBits);
        gorillaDecoder = new GorillaDecoder();
    }

    public GorillaDecoder getGorillaDecoder() {
        return gorillaDecoder;
    }

    public List<Double> getValues() throws IOException {
        List<Double> list = new LinkedList<>();
        Double value = next();
        while (value != null) {
            list.add(value);
            value = next();
        }
        return list;
    }

    private Double next() throws IOException {
        if (in.availableBits() <= 0) return null;
        double retVal = 0;
        if (isFirst) {
            isFirst = false;
            long fistValLong = in.readLong(64);
            double firstVal = Double.longBitsToDouble(fistValLong);
            storedVal = (long)firstVal;
            retVal = firstVal;
        } else {
            retVal = nextValue();
        }
        previousValue = Double.doubleToLongBits(retVal);
        return retVal;
    }

    private Double nextValue() throws IOException {
        boolean positive = in.readBit();
        double posVal = positive ? -1.0 : 1.0;
        boolean useCamel = in.readBit();
        if (useCamel) {
            long longVal = readLong();
            double decimal = readDecimal();
            if (longVal >= 0) {
                return posVal * (longVal + decimal);
            } else {
                return posVal * -1 * (-1 * longVal + decimal);
            }
        } else {
            return posVal * gorillaDecoder.decode(in);
        }
    }

    // 解压整数部分
    private long readLong() throws IOException {
        long diffVal = BitInputStream.readVarLong(in);
        storedVal = diffVal + storedVal;
        return  storedVal;

    }

    // 解压小数部分
    private double readDecimal() throws IOException {
        // 读取小数位数
        int decimalCount = in.readInt(4) + 1;
        // 是否计算m的值
        int isCalM = in.readInt(1);
        long xor;
        double decimalVal, m;
        long xorString = 0;
        if (isCalM == 1) {
            // 查找保存的xor值
            xor = in.readInt(decimalCount);
            // 根据leadingZeroSNum和XOR拼接xorVal
            long shiftedValue = xor << (52 - decimalCount);
            for (int i = 0; i < 64; i++) {
                xorString ^= (shiftedValue & (1L << i)); // 使用异或操作符直接计算xorValue
            }
        }
        long m_long = BitInputStream.readVarLong(in);
        if (isCalM == 1){
            m = (double) m_long / powers[decimalCount-1] + 1;
            long m_prime = Double.doubleToLongBits(m);
            long decimalLong = xorString ^ m_prime;;
            double temp = Double.longBitsToDouble(decimalLong) - 1;
            double scale = Math.pow(10, decimalCount);
            decimalVal = Math.round(temp * scale) / scale;
        } else {
            m = (double) m_long / powers[decimalCount-1];
            decimalVal = m;
        }
        return decimalVal;
    }
}

