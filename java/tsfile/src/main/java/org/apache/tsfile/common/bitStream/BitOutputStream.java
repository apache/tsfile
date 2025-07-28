package org.apache.tsfile.common.bitStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A bit-level output stream that writes bits to an underlying byte-oriented OutputStream.
 * Bits are written in MSB-first (most significant bit first) order within each byte.
 */
public class BitOutputStream extends BitStream {

    protected OutputStream out;
    protected int buffer;             // Bit buffer (8-bit)
    protected int bufferBitCount;     // Number of bits currently in the buffer

    protected int bitsWritten = 0;    // Total number of bits written

    /**
     * Constructs a BitOutputStream from the given OutputStream.
     *
     * @param out the underlying OutputStream
     */
    public BitOutputStream(OutputStream out) {
        this.out = out;
        this.bufferBitCount = 0;
    }

    /**
     * Writes the specified number of bits from the given integer.
     * Bits are taken from the lower bits of the data and written MSB-first.
     *
     * @param data     the data to write (bits from the LSB end)
     * @param numBits  number of bits to write (0–32)
     * @throws IOException if an I/O error occurs
     */
    public void writeInt(int data, int numBits) throws IOException {
        bitsWritten += numBits;
        while (numBits > 0) {
            int rest = 8 - bufferBitCount;

            if (rest > numBits) {
                buffer |= ((data & MASKS[numBits]) << (rest - numBits));
                bufferBitCount += numBits;
                numBits = 0;
            } else {
                buffer |= ((data >> (numBits - rest)) & MASKS[rest]);
                out.write(buffer);
                buffer = 0;
                bufferBitCount = 0;
                numBits -= rest;
            }
        }
    }

    /**
     * Writes the specified number of bits from the given long value.
     * Bits are taken from the lower bits of the data and written MSB-first.
     *
     * @param data     the data to write (bits from the LSB end)
     * @param numBits  number of bits to write (0–64)
     * @throws IOException if an I/O error occurs
     */
    public void writeLong(long data, int numBits) throws IOException {
        if (numBits > 64 || numBits < 0) {
            throw new IllegalArgumentException("numBits must be between 0 and 64");
        }

        bitsWritten += numBits;
        while (numBits > 0) {
            int rest = 8 - bufferBitCount;

            if (rest > numBits) {
                int shift = rest - numBits;
                int toWrite = (int)((data & MASKS[numBits]) << shift);
                buffer |= toWrite;
                bufferBitCount += numBits;
                numBits = 0;
            } else {
                int shift = numBits - rest;
                int toWrite = (int)((data >> shift) & MASKS[rest]);
                buffer |= toWrite;
                out.write(buffer);
                buffer = 0;
                bufferBitCount = 0;
                numBits -= rest;
            }
        }
    }

    public static int writeVarInt(int value, BitOutputStream out) throws IOException {
        int uValue = (value << 1) ^ (value >> 31);  // ZigZag 编码：正偶负奇
        int bits = 0;

        while ((uValue & ~0x7F) != 0) {
            out.writeInt(uValue & 0x7F, 7); // 写低 7 位
            out.writeBit(true);             // 后续标志位 1
            uValue >>>= 7;
            bits += 8;
        }

        out.writeInt(uValue, 7);           // 最后一组 7 位
        out.writeBit(false);               // 终止标志位 0
        bits += 8;

        return bits;
    }

    public static int writeVarLong(long value, BitOutputStream out) throws IOException {
        long uValue = (value << 1) ^ (value >> 63); // ZigZag 编码：正偶负奇
        int bitsWritten = 0;

        while ((uValue & ~0x7FL) != 0) {
            int chunk = (int)(uValue & 0x7F);     // 低 7 位
            out.writeInt(chunk, 7);               // 写数据位
            out.writeBit(true);                   // 还有后续
            uValue >>>= 7;
            bitsWritten += 8;
        }

        out.writeInt((int)(uValue & 0x7F), 7);    // 最后一个字节
        out.writeBit(false);                     // 结束标志
        bitsWritten += 8;
        return bitsWritten;
    }


    /**
     * Writes a single bit.
     * The bit is stored in the buffer until a full byte is collected.
     *
     * @param bit true to write 1, false to write 0
     * @throws IOException if an I/O error occurs
     */
    public void writeBit(boolean bit) throws IOException {
        bitsWritten += 1;

        buffer |= (bit ? 1 : 0) << (7 - bufferBitCount);
        bufferBitCount++;

        if (bufferBitCount == 8) {
            out.write(buffer);
            buffer = 0;
            bufferBitCount = 0;
        }
    }

    /**
     * Flushes the remaining bits in the buffer to the stream (if any),
     * padding the remaining bits with zeros in the lower positions.
     *
     * @throws IOException if an I/O error occurs
     */
    public void close() throws IOException {
        if (bufferBitCount > 0) {
            out.write(buffer);
        }
        out.close();
    }

    /**
     * Returns the total number of bits written so far.
     *
     * @return the number of bits written
     */
    public int getBitsWritten() {
        return bitsWritten;
    }
}