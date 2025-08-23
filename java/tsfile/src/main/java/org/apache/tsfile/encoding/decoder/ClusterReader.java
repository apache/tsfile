package org.apache.tsfile.encoding.decoder;

import java.nio.ByteBuffer;

public class ClusterReader {
    private final ByteBuffer buffer;
    private byte currentByte;
    private int bitPosition; // from 7 down to 0

    public ClusterReader(ByteBuffer buffer) {
        this.buffer = buffer;
        this.currentByte = 0;
        this.bitPosition = -1; // Start at -1 to force reading a new byte first
    }

    public long read(int numBits) {
        if (numBits > 64 || numBits <= 0) {
            throw new IllegalArgumentException("Cannot read more than 64 bits or non-positive bits at once.");
        }

        long result = 0;
        for (int i = 0; i < numBits; i++) {
            if (bitPosition < 0) {
                currentByte = buffer.get();
                bitPosition = 7;
            }
            // Read the bit at the current position
            long bit = (currentByte >> bitPosition) & 1;
            // Shift the result and add the new bit
            result = (result << 1) | bit;
            bitPosition--;
        }
        return result;
    }
}