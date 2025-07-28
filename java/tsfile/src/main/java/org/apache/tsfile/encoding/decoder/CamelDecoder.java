package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.common.bitStream.BitInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

public class CamelDecoder {

    public static class GorillaDecoder {
        private long previousValue = 0;
        private int leadingZeros = Integer.MAX_VALUE;
        private int trailingZeros = 0;
        private boolean isFirst = true;

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
                if (significantBits == 0) {
                    //return Double.longBitsToDouble(previousValue); // no change
                }
                trailingZeros = 64 - leadingZeros - significantBits;
                xor = in.readLong(significantBits) << trailingZeros;
            }

            previousValue ^= xor;
            return Double.longBitsToDouble(previousValue);
        }
    }


    private final BitInputStream in;

    private long storedVal = 0;

    private boolean first = true;

    private final static int DECIMAL_MAX_COUNT = 10;
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
        if (first) {
            first = false;
            long fistVal_long = in.readLong(64);
            double firstVal = Double.longBitsToDouble(fistVal_long);
            storedVal = (int)firstVal;
            return firstVal;
        } else {
            return nextValue();
        }
    }

    private Double nextValue() throws IOException {
        // 读取第一位符号位 0表示负数 1表示正数
        long longVal = readLong();
        double decimal = readDecimal();
        if (longVal >= 0) {
            return longVal + decimal;
        } else {
            return -1 * (-1 * longVal + decimal);
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
        int decimal_count = in.readInt(2) + 1;
        // 是否计算m的值
        int isCalM = in.readInt(1);
        long xor;
        double decimalVal, m;
        long xorString = 0;
        if (isCalM == 1) {
            // 查找保存的xor值
            xor = in.readInt(decimal_count);
            // 根据leadingZeroSNum和XOR拼接xorVal
            long shiftedValue = xor << (52 - decimal_count);
            for (int i = 0; i < 64; i++) {
                xorString ^= (shiftedValue & (1L << i)); // 使用异或操作符直接计算xorValue
            }
        }
        long m_long = BitInputStream.readVarLong(in);
        if (isCalM == 1){
            m = (double) m_long / powers[decimal_count-1] + 1;
            long m_prime = Double.doubleToLongBits(m);
            long decimalLong = xorString ^ m_prime;;
            decimalVal = Double.longBitsToDouble(decimalLong) - 1;
        } else {
            m = (double) m_long / powers[decimal_count-1];
            decimalVal = m;
        }
        return decimalVal;
    }
}

