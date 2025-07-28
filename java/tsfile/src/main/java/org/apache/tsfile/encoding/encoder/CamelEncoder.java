package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.common.bitStream.BitOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CamelEncoder {
    public static GorillaEncoder GorillaEncoder;

    public enum CamelInnerEncodingType {
        CAMEL(0),
        Gorilla(1);

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

    public static class GorillaEncoder {
        private long previousValue = 0;
        private int leadingZeros = Integer.MAX_VALUE;
        private int trailingZeros = 0;
        private boolean isFirst = true;

        public void encode(double value, BitOutputStream out) throws IOException {
            long curr = Double.doubleToLongBits(value);

            if (isFirst) {
                out.writeLong(curr, 64);
                previousValue = curr;
                isFirst = false;
                return;
            }

            long xor = curr ^ previousValue;
            if (xor == 0) {
                out.writeBit(false); // Control bit
            } else {
                out.writeBit(true); // Control bit
                int leading = Long.numberOfLeadingZeros(xor);
                int trailing = Long.numberOfTrailingZeros(xor);
                if (leading >= leadingZeros && trailing >= trailingZeros) {
                    out.writeBit(false); // Reuse previous block
                    int significantBits = 64 - leadingZeros - trailingZeros;
                    out.writeLong(xor >>> trailingZeros, significantBits);
                } else {
                    out.writeBit(true); // Write new leading/trailing info
                    out.writeInt(leading, 6);
                    int significantBits = 64 - leading - trailing;
                    out.writeInt(significantBits - 1, 6);
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

    private boolean first = true;

    private int size;

    private final static int DECIMAL_MAX_COUNT = 15;

    private  boolean decimalCountFlag = false;

    private  int decimalCount = 0;

    double prevVal = 0;

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
        prevVal = value;
        if(first) {
            return writeFirst(Double.doubleToRawLongBits(value));
        } else {
            return compressValue(value);
        }
    }

    // 写入第一个数据
    private int writeFirst(long value) throws IOException {
        first = false;
        // 保存第一个数字的整数进行差值计算
        storedVal = (int) Double.longBitsToDouble(value);
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
        compressIntegerValue((long)value);
        double factor = 1;
        value = Math.abs(value);
        if (!decimalCountFlag) {
            double epsilon = 0.0000001; // 设置一个很小的阈值
            while (Math.abs(value * factor - Math.round(value * factor)) > epsilon) {
                factor *= 10.0;
                decimalCount++;
            }
            decimalCountFlag = true;
        }

        long decimalValue;
        if (decimalCount == 0) {
            decimalCount = 1;
        }

        if (decimalCount > 0 && decimalCount <= DECIMAL_MAX_COUNT) {
            long pow = powers[decimalCount - 1];
            decimalValue = Math.round(value * pow) % pow;
        }else {
            decimalValue = ((long) (value * powers[DECIMAL_MAX_COUNT]) % powers[DECIMAL_MAX_COUNT])/10;
            decimalCount = DECIMAL_MAX_COUNT;
        }
        compressDecimalValue(decimalValue, decimalCount);
        return this.size;
    }

    // 压缩小数部分
    private void compressDecimalValue(long decimal_value, int decimalCount) throws IOException {
        out.writeInt(decimalCount-1, 2);
        size += 2;
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


    public int getSize() {
        return size;
    }
}
