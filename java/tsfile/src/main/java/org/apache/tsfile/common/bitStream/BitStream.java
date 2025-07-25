package org.apache.tsfile.common.bitStream;

/**
 * Base class for bit-level stream operations.
 * Provides shared constants and bit masks for bitwise manipulation.
 */
public class BitStream {

    /** Number of bits per byte (always 8) */
    protected static final int BITS_PER_BYTE = 8;

    /**
     * Bit masks used to extract the lowest N bits of a value.
     * MASKS[n] contains a bitmask with the lowest n bits set to 1.
     * For example:
     * MASKS[0] = 0b00000000
     * MASKS[1] = 0b00000001
     * MASKS[2] = 0b00000011
     * ...
     * MASKS[8] = 0b11111111
     */
    protected static final int[] MASKS = new int[] {
            0, 1, 3, 7, 0xf, 0x1f, 0x3f, 0x7f, 0xff
    };
}