package it.unipi.dii.mircv.yase.compression;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 *
 *  A class for <a href="https://en.wikipedia.org/wiki/Unary_coding">Unary</a> compression/decompression of arrays of
 *  integers.
 *  The padding at the end cannot be mistaken for additional integers if we know the length of the list, as in the
 *  case of our posting lists.
 *  Example: [3, 1, 2] ==> code -> {110010}|{00} <- padding
 */
public class Unary {

    /**
     * method to encode an array of integers in an array of bytes.
     * @param intStream the array of integers
     * @return the array of bytes
     */
    public static byte[] encodeIntegerArray(int[] intStream) {
        // iterate on the array of integers
        BitSet encoding = new BitSet();
        int fromIndex = 0;
        for (int i = 0; i < intStream.length; i++) {
            if (intStream[i] > 1)
                encoding.set(fromIndex, fromIndex + intStream[i]-1);
            fromIndex += intStream[i];
        }

        return encoding.toByteArray();
    }

    /**
     * Method used in order to decode a single integer from a compressed buffer.
     *
     * @param buffer the buffer compressed with Unary encoding.
     * @param bitPosition the position from which we start to decode.
     * @return the decoded integer
     */
    public static int getIntegerFromBuffer(BitSet buffer, int bitPosition) {
        // compute the integer by index difference
        return buffer.nextClearBit(bitPosition) - bitPosition + 1;
    }
}
