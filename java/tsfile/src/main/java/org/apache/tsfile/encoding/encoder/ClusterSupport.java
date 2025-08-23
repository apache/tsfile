package org.apache.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ClusterSupport {

    private final ByteArrayOutputStream out;
    private byte currentByte;
    private int bitPosition; // 0-7, from left to right (MSB to LSB)

    public ClusterSupport(ByteArrayOutputStream out) {
        this.out = out;
        this.currentByte = 0;
        this.bitPosition = 0;
    }

    /**
     * Writes a value using a specified number of bits.
     *
     * @param value The long value to write. Only the lower `numBits` will be used.
     * @param numBits The number of bits to write for the value (must be > 0 and <= 64).
     * @throws IOException If an I/O error occurs.
     */
    public void write(long value, int numBits) throws IOException {
        if (numBits <= 0 || numBits > 64) {
            throw new IllegalArgumentException("Number of bits must be between 1 and 64.");
        }
        for (int i = numBits - 1; i >= 0; i--) {
            // Get the i-th bit from the value
            boolean bit = ((value >> i) & 1) == 1;
            writeBit(bit);
        }
    }

    private void writeBit(boolean bit) throws IOException {
        if (bit) {
            currentByte |= (1 << (7 - bitPosition));
        }
        bitPosition++;
        if (bitPosition == 8) {
            out.write(currentByte);
            currentByte = 0;
            bitPosition = 0;
        }
    }

    /**
     * Flushes any remaining bits in the current byte to the output stream.
     * This must be called at the end to ensure all data is written.
     * @throws IOException If an I/O error occurs.
     */
    public void flush() throws IOException {
        if (bitPosition > 0) {
            out.write(currentByte);
        }
        // It's good practice to reset, though not strictly necessary if the instance is discarded.
        currentByte = 0;
        bitPosition = 0;
    }

    /**
     * A helper to calculate the number of bits required for a non-negative long value.
     *
     * @param value The non-negative value.
     * @return The number of bits required to represent it.
     */
    public static int bitsRequired(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Value must be non-negative.");
        }
        if (value == 0) {
            return 1;
        }
        return 64 - Long.numberOfLeadingZeros(value);
    }
}
