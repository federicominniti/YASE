package it.unipi.dii.mircv.yase.compression;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UnaryTest {
    final int LENGTH = 100000;
    final int MAX_VALUE = 20;
    int[] randomDecodedArray;
    byte[] randomEncodedArray;

    @BeforeEach
    void init() {
        Random r = new Random();
        randomDecodedArray = new int[LENGTH];
        for (int i = 0; i < LENGTH; i++) {
            randomDecodedArray[i] = r.nextInt(MAX_VALUE) + 1;
        }
        randomEncodedArray = Unary.encodeIntegerArray(randomDecodedArray);
    }

    @Test
    @DisplayName("Test Unary buffer decoding")
    void testEncodingListOfInteger() {
        List<Integer> decodedList = new ArrayList<>();
        int offset = 0;
        BitSet bitSet = BitSet.valueOf(randomEncodedArray);
        for (int i = 0; i < LENGTH; i++) {
            int decoded = Unary.getIntegerFromBuffer(bitSet, offset);
            offset += decoded;
            decodedList.add(decoded);
        }
        assertEquals(Ints.asList(randomDecodedArray), decodedList);
    }

}