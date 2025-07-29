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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.common.bitStream.BitOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CamelEncoder {
    private GorillaEncoder gorillaEncoder;

    public enum CamelInnerEncodingType {
        GORILLA(0),
        CAMEL(1);

        private final int code;

        CamelInnerEncodingType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static CamelInnerEncodingType fromCode(int code) {
            for (CamelInnerEncodingType type : CamelInnerEncodingType.values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid encoding type code: " + code);
        }
    }

    public class GorillaEncoder {
        private int leadingZeros = Integer.MAX_VALUE;
        private int trailingZeros = 0;

        public void encode(double value, BitOutputStream out) throws IOException {
            long curr = Double.doubleToLongBits(value);
            if (isFirst) {
                size += 64;
                out.writeLong(curr, 64);
                previousValue = curr;
                isFirst = false;
                return;
            }

            long xor = curr ^ previousValue;
            size += 1;
            if (xor == 0) {
                out.writeBit(false); // Control bit
            } else {
                out.writeBit(true); // Control bit
                int leading = Long.numberOfLeadingZeros(xor);
                int trailing = Long.numberOfTrailingZeros(xor);
                size += 1;
                if (leading >= leadingZeros && trailing >= trailingZeros) {
                    out.writeBit(false); // Reuse previous block
                    int significantBits = 64 - leadingZeros - trailingZeros;
                    size += significantBits;
                    out.writeLong(xor >>> trailingZeros, significantBits);
                } else {
                    out.writeBit(true); // Write new leading/trailing info
                    size += 6;
                    out.writeInt(leading, 6);
                    int significantBits = 64 - leading - trailing;
                    size += 6;
                    out.writeInt(significantBits - 1, 6);
                    size += significantBits;
                    out.writeLong(xor >>> trailing, significantBits);
                    leadingZeros = leading;
                    trailingZeros = trailing;
                }
            }

            previousValue = curr;
        }

        public void close (BitOutputStream out) throws IOException {
            out.close();
        }
    }


    private long storedVal = 0;

    private boolean isFirst = true;

    private int size;

    private final static int DECIMAL_MAX_COUNT = 15;

    private  int decimalCount = 0;

    long previousValue = 0;

    public static final long[] powers = new long[DECIMAL_MAX_COUNT];

    // threshold[l-1] = 10^l / 2^l
    public static final long[] threshold = new long[DECIMAL_MAX_COUNT];

    // mValueBits[l-1] = ceil(log2(threshold[l-1]))
    public static final int[] mValueBits = new int[DECIMAL_MAX_COUNT];

    public static Map<String, byte[]> compressVal = new HashMap<>();

    private final BitOutputStream out;
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public CamelEncoder() {
        for (int l = 1; l <= DECIMAL_MAX_COUNT; l++) {
            int idx = l - 1;
            powers[idx] = (long) Math.pow(10, l);
            long divisor = 1L << l;  // 2^l
            threshold[idx] = powers[idx] / divisor;
            mValueBits[idx] = (int) Math.ceil(Math.log(threshold[idx]) / Math.log(2));
        }
        out = new BitOutputStream(baos);
        size = 0;
        gorillaEncoder = new GorillaEncoder();
    }

    public GorillaEncoder getGorillaEncoder() {
        return gorillaEncoder;
    }

    public ByteArrayOutputStream getByteArrayOutputStream() {
        return this.baos;
    }

    /**
     * Adds a new double value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public int addValue(double value) throws IOException {
        if(isFirst) {
            writeFirst(Double.doubleToRawLongBits(value));
        } else {
            compressValue(value);
        }
        previousValue = Double.doubleToLongBits(value);
        return size;
    }

    // 写入第一个数据
    private int writeFirst(long value) throws IOException {
        isFirst = false;
        // 保存第一个数字的整数进行差值计算
        storedVal = (long) Double.longBitsToDouble(value);
        out.writeLong(value, 64);
        size += 64;
        return size;
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    public long close() throws IOException {
        out.close();
        return out.getBitsWritten();
    }

    // 数据压缩
    private int compressValue(double value) throws IOException {
        size += 1;
        byte signBit = (byte) ((Double.doubleToLongBits(value) >>> 63) & 1);
        out.writeBit(signBit == 1);

        value = Math.abs(value);
        if (value > Long.MAX_VALUE || value == 0 || Math.abs(Math.floor(Math.log10(value))) > DECIMAL_MAX_COUNT) {
            // gorilla
            size += 1;
            out.writeInt(CamelInnerEncodingType.GORILLA.getCode(), 1);
            gorillaEncoder.encode(value, out);
            return this.size;
        }

        // 计算整数部分的值和十进制位数
        long integerPart = (long) value;
        int numDigits = integerPart == 0 ? 1 : (int) Math.log10(Math.abs(integerPart)) + 1;
        // 计算小数部分的位数和值
        double factor = 1;
        while (Math.abs(value * factor - Math.round(value * factor)) > 0) {
            factor *= 10.0;
            decimalCount++;
            if (decimalCount > DECIMAL_MAX_COUNT) {
                break;
            }
        }
        decimalCount = Math.max(1, decimalCount);
        long decimalValue = 0;
        if (decimalCount + numDigits <= DECIMAL_MAX_COUNT) {
            long pow = powers[decimalCount - 1];
            decimalValue = Math.round(value * pow) % pow;
            size += 1;
            // camel
            out.writeInt(CamelInnerEncodingType.CAMEL.getCode(), 1);
            compressIntegerValue(integerPart);
            compressDecimalValue(decimalValue, decimalCount);
        } else {
            // gorilla
            size += 1;
            out.writeInt(CamelInnerEncodingType.GORILLA.getCode(), 1);
            gorillaEncoder.encode(value, out);
        }

        return this.size;
    }

    // 压缩小数部分
    private void compressDecimalValue(long decimal_value, int decimalCount) throws IOException {
        out.writeInt(decimalCount-1, 4);
        size += 4;
        // 计算m的值
        long thread = threshold[decimalCount-1];
        int m = (int) decimal_value;
        size += 1;
        if (decimal_value - thread >= 0) {  // 计算m的值
            // 标志位：是否计算m的值
            out.writeBit(true);
            m = (int) (decimal_value % thread);
            // 对于m进行XOR操作
            long xor = (Double.doubleToLongBits((double)decimal_value/powers[decimalCount-1]+1)) ^ Double.doubleToLongBits(((double) m/powers[decimalCount-1]+1));
            // 保存小数位数长度的centerBits
            out.writeLong(xor >>> 52 - decimalCount, decimalCount);
            size += decimalCount;// Store the meaningful bits of XOR

        } else {
            out.writeBit(false);
        }
        this.size += BitOutputStream.writeVarLong(m, out);
    }

    // 压缩整数部分
    private void compressIntegerValue(long value) throws IOException {
        long diff = value - storedVal;
        storedVal = value;
        int bitsWritten = BitOutputStream.writeVarLong(diff, out);
        this.size += bitsWritten;
    }


    public int getWrittenBits() {
        return out.getBitsWritten();
    }
}
