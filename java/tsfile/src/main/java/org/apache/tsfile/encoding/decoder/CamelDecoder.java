package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.common.bitStream.BitInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;

public class CamelDecoder {

    private final BitInputStream in;

    private long storedVal = 0;

    private boolean first = true;

    private boolean endOfStream = false;

    private long totalBits = 0;

    private static final int DEFAULT_BLOCK_SIZE = 1000;
    private static final long[] powers = {10L, 100L, 1000L, 10000L};
    private long readNum = 0;

    // 1位小数对应的centerbits与前导数0的关系
    public final static int[] leadingZerosNumOne = {12};
    public final static int[] centerBitsNumOne = {1};

    // 2位小数对应的centerbits与前导数0的关系
    public final static int[] leadingZerosNumTwo = {13, 12};
    public final static int[] centerBitsNumTwo = {1, 2};

    // 3位小数对应的centerbits与前导数0的关系
    public final static int[] centerBitsNumThree = {1, 2, 3};
    public final static int[] leadingZerosNumThree = {14, 13, 12};

    // 4位小数对应的centerbits与前导数0的关系
    public final static int[] centerBitsNumFour = {1, 2, 3, 4};
    public final static int[] leadingZerosNumFour = {15, 14, 13, 12};

    // 5位小数对应的centerbits与前导数0的关系
    public final static int[] centerBitsNumFive = {1, 2, 3, 4, 5};
    public final static int[] leadingZerosNumFive  = {16, 15, 14, 13, 12};

    // 按照寻找到的m的值进行保存
    public final static int[] mValueBits = {3, 5, 7, 10, 15};

    public CamelDecoder(InputStream in, long totalBits) {
        this.in = new BitInputStream(in, totalBits);
        this.totalBits = totalBits;
    }

    public List<Double> getValues() {
        List<Double> list = new LinkedList<>();
        Double value = readPair();
        while (value != null) {
            list.add(value);
            value = readPair();
        }
        return list;
    }

    public Double readPair() {
        Double decoded_val;
        try {
            decoded_val = next();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        if(endOfStream) {
            return null;
        }
        return decoded_val;
    }

    private Double next() throws IOException {
        if (in.availableBits() <= 0) return null;
        readNum ++;
        if (first) {
            first = false;
            long fistVal_long = in.readLong(64);
            double firstVal = Double.longBitsToDouble(fistVal_long);
            storedVal = (int)firstVal;
            return firstVal;
        } else {
            if (readNum-1 == DEFAULT_BLOCK_SIZE) {
                endOfStream = true;
                return null;
            }
            return nextValue();
        }
    }

    private Double nextValue() throws IOException {
        // 读取第一位符号位 0表示负数 1表示正数
        long intVal = readInt();
        double decimal = readDecimal();
        if (intVal >= 0) {
            return intVal + decimal;
        } else {
            return -1 * (-1 * intVal + decimal);
        }
    }

    // 解压整数部分
    private long readInt() throws IOException {
        int integerNum = in.readInt(2);
        long diffVal;
        if (integerNum < 3) {
            diffVal = integerNum-1;
        } else {
            // 读取差值的符号 0表示负数 1表示正数
            boolean diffSymbol = in.readBit();
            int range = in.readInt(1);
            if (range == 0) {
                diffVal = in.readInt(3);
            } else {
                diffVal = in.readInt(32);
            }
            diffVal = (!diffSymbol ? -1: 1) * diffVal;
        }
        storedVal = diffVal + storedVal;
        return  storedVal;

    }


    // 解压小数部分
    private double readDecimal() throws IOException {
        // 读取小数位数
        int decimal_count = in.readInt(2) + 1;
        // 是否计算m的值
        int isM = in.readInt(1);
        long xor;
        double decimalVal, m;
        long xorString = 0;
        if (isM == 1) {
            // 查找保存的xor值
            xor = in.readInt(decimal_count);
            // 根据leadingZeroSNum和XOR拼接xorVal
            long shiftedValue = xor << (52 - decimal_count);
//            xorString = shiftedValue;
//            xorString = String.format("%64s", Long.toBinaryString(shiftedValue)).replace(' ', '0');
            for (int i = 0; i < 64; i++) {
                xorString ^= (shiftedValue & (1L << i)); // 使用异或操作符直接计算xorValue
            }
        }
        // 将m用二进制数表示
        int m_int = 0;
        if (decimal_count <= 1) { // 如果是1 直接往后读decimal_count+1位
            m_int = in.readInt(decimal_count+1);
        } else if (decimal_count ==2) {
            int temp = in.readInt(1);
            if ( temp == 0) {
                m_int = in.readInt(3);
            }  else {
                m_int = in.readInt(5);
            }
        } else if (decimal_count == 3) {
            int temp = in.readInt(2);
            if ( temp == 0) {
                m_int = in.readInt(1);
            }  else if (temp == 1) {
                m_int = in.readInt(3);
            } else if (temp == 2) {
                m_int = in.readInt(5);
            } else {
                m_int = in.readInt(mValueBits[decimal_count-1]);
            }
        } else {
            int temp = in.readInt(2);
            if (temp == 0) {
                m_int = in.readInt(4);
            } else if (temp == 1) {
                m_int = in.readInt(6);
            } else if (temp == 2) {
                m_int = in.readInt(8);
            } else {
                m_int = in.readInt(mValueBits[decimal_count - 1]);
            }

        }

        if (isM == 1){
            m = (double) m_int / powers[decimal_count-1] + 1;
            long m_prime = Double.doubleToLongBits(m);
            long decimalLong = xorString ^ m_prime;;
            // 使用 Double.longBitsToDouble 将 long 转换为 double
            decimalVal = Double.longBitsToDouble(decimalLong) - 1;
        } else {
            m = (double) m_int / powers[decimal_count-1];
            decimalVal = m;
        }

        return decimalVal;

    }

    public static String xorBinaryStrings(String binary1, String binary2) {
        // 确保两个二进制字符串长度一致
        int maxLength = Math.max(binary1.length(), binary2.length());
        binary1 = String.format("%" + maxLength + "s", binary1).replace(' ', '0');
        binary2 = String.format("%" + maxLength + "s", binary2).replace(' ', '0');

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < maxLength; i++) {
            if (binary1.charAt(i) == binary2.charAt(i)) {
                result.append('0');
            } else {
                result.append('1');
            }
        }
        return result.toString();
    }

    public long getTotalBits() {
        return totalBits;
    }

    public void setTotalBits(long totalBits) {
        this.totalBits = totalBits;
    }
}

