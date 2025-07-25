package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.common.bitStream.BitOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class CamelEncoder {

    private long storedVal = 0;

    // 默认10000 对应 block大小位1000
    private final static int outStreamSize = 100000;
    private boolean first = true;
    private int size;
    private final static long END_SIGN = Double.doubleToLongBits(Double.NaN);

    private final static int DECIMAL_MAX_COUNT = 4;

    private  boolean decimalCountFlag = false;

    private  int decimal_count = 0;

    // 按照寻找到的m的值进行保存
    public final static int[] mValueBits = {3, 5, 7, 10, 12, 14};
    //    public final static BigDecimal[]  threshold = {BigDecimal.valueOf(0.5), BigDecimal.valueOf(0.25), BigDecimal.valueOf(0.125), BigDecimal.valueOf(0.0625)};
    public final static long[]  threshold = {5, 25, 125, 625, 3125, 15625};

    private static final long[] powers = {10L, 100L, 1000L, 10000L, 10000L, 100000L, 1000000L};
    public static Map<String, byte[]> compressVal = new HashMap<>();

    private final BitOutputStream out;
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // We should have access to the series?
    public CamelEncoder() {
        out = new BitOutputStream(baos);
        size = 0;
    }

    public ByteArrayOutputStream getByteArrayOutputStream() {
        return this.baos;
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public int addValue(long value) throws IOException {
        if(first) {
            return writeFirst(value);
        } else {
            return compressValue(value);
        }
    }

    /**
     * Adds a new double value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public int addValue(double value) throws IOException {
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
//        compressVal.put("compressInt", convertToBinary((int) value, 64));
        return size;
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    public long close() throws IOException {
//        addValue(END_SIGN);
        out.close();
        long totalWrittenBits = out.getBitsWritten();
        return totalWrittenBits;
    }

    // 数据压缩
    private int compressValue(double value) throws IOException {
        // 压缩小数位 默认小数位是1.**
        size = compressIntegerValue((int)value);
        double factor = 1;
        value = Math.abs(value);
        if (!decimalCountFlag) {
            double epsilon = 0.0000001; // 设置一个很小的阈值
            while (Math.abs(value * factor - Math.round(value * factor)) > epsilon) {
                factor *= 10.0;
                decimal_count++;
            }
            decimalCountFlag = true;
        }

        long decimal_value;
        if (decimal_count == 0) {
            decimal_count = 1;
        }

        if (decimal_count > 0 && decimal_count<= DECIMAL_MAX_COUNT) {
            decimal_value =  ((long) (value * powers[decimal_count]) % powers[decimal_count])/10;
        }else {
            decimal_value = ((long) (value * powers[DECIMAL_MAX_COUNT]) % powers[DECIMAL_MAX_COUNT])/10;
            decimal_count = DECIMAL_MAX_COUNT;
        }
        size = compressDecimalValue(decimal_value, decimal_count);


        // 压缩整数位

        return size;
    }


    public int countDecimalPlaces(BigDecimal value) {
        String valueStr = value.toString();
        int decimalPointIndex = valueStr.indexOf('.');

        if (decimalPointIndex >= 0) {
            return valueStr.length() - decimalPointIndex - 1;
        } else {
            // No decimal point, so there are no decimal places
            return 0;
        }
    }


    // 压缩小数部分
    private int compressDecimalValue(long decimal_value, int decimal_count) throws IOException {
        // 计算小数位数
        out.writeInt(decimal_count-1, 2); // 保存字节数 00-1 01-2 10-3 11-4
        size += 2;
        // 计算m的值
        long thread = threshold[decimal_count-1];
        int m = (int) decimal_value;
        size += 1;
        if (decimal_value - thread >= 0) {  // 计算m的值
            // 标志位：是否计算m的值
            out.writeBit(true);
            m = (int) (decimal_value % thread);
            // 对于m进行XOR操作
            long xor = (Double.doubleToLongBits((double)decimal_value/powers[decimal_count-1]+1)) ^ Double.doubleToLongBits(((double) m/powers[decimal_count-1]+1));
            // 保存小数位数长度的centerBits 保存decimal_count （四位最多就是1000）
            out.writeLong(xor >>> 52 - decimal_count, decimal_count);
            size += decimal_count;// Store the meaningful bits of XOR

        } else {  // m就为原来的值
            out.writeBit(false);
        }

        // 保存m的值
        if (decimal_count <= 1) { // 如果是1 直接往后读decimal_count+1位
            out.writeInt(m, decimal_count + 1);
            size += decimal_count + 1;
            return this.size;
        }
        if (decimal_count ==2) {
            if (m < 8) {
                out.writeInt(0, 1);
                out.writeInt(m, 3);
                size += 4;
                return this.size;
            }  else {
                out.writeInt(1, 1);
                out.writeInt(m, 5);
                size += 6;
                return this.size;
            }
        }
        if (decimal_count == 3) {
            if (m < 2) {
                out.writeInt(0, 2);
                out.writeInt(m, 1);
                size += 3;
                return this.size;
            }else if (m < 8){
                out.writeInt(1, 2);
                out.writeInt(m, 3);
                size += 5;
                return this.size;
            }else if (m < 32) {
                out.writeInt(2, 2);
                out.writeInt(m, 5);
                size += 7;
                return this.size;
            }else {
                out.writeInt(3, 2);
                out.writeInt(m, mValueBits[decimal_count-1]);
                size += 2;
                size += mValueBits[decimal_count-1];
                return this.size;
            }
        }
        if (decimal_count >= 4){
            if (m < 16) {
                out.writeInt(0, 2);
                out.writeInt(m, 4);
                size += 6;
                return this.size;
            }else if (m < 64){
                out.writeInt(1, 2);
                out.writeInt(m, 6);
                size += 8;
                return this.size;
            }else if (m < 256) {
                out.writeInt(2, 2);
                out.writeInt(m, 8);
                size += 10;
                return this.size;
            }else {
                out.writeInt(3, 2);
                out.writeInt(m, mValueBits[decimal_count-1]);
                size += 2;
                size += mValueBits[decimal_count-1];
                return this.size;
            }

        }

        return this.size;

    }

    // 压缩整数部分
    private int compressIntegerValue(long int_value) throws IOException {

        int diff = (int)(int_value - storedVal) ;
        size += 2;
        storedVal = int_value;
        if (diff >= -1 && diff <= 1) {
            out.writeInt((diff + 1), 2); // Map -1 to 0, 0 to 1, 1 to 2 respectively
            return this.size;
        } else{
            out.writeInt(3, 2); // //11
            if (diff < 0){
                out.writeBit(false);
                diff = -diff;
            } else {
                out.writeBit(true);

            }
            size += 1;
            if (diff >=2 && diff < 8) { // [4,8)
                out.writeInt(0, 1); // 0
                out.writeInt(diff, 3);
                size += 4;
                return this.size;
            } else {
                out.writeInt(1, 1); //1  // [8,...)
                out.writeInt(diff, 32); // 暂用16个bit表示
                size += 17;
                return this.size;
            }
        }
//        return this.size;
    }


    public int getSize() {
        return size;
    }
}
