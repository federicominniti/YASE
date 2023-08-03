package it.unipi.dii.mircv.yase.compression;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VariableByteTest {
    final int MAX_VALUE = 1000;
    @Test
    @DisplayName("Test encoding")
    void testEncoding() {
        byte[] result = VariableByte.encodeInteger(777);
        assertEquals((byte)0b00000110, result[0]);
        assertEquals((byte)0b10001001, result[1]);
    }

    @Test
    @DisplayName("Test decoding")
    void testDecoding() {
        byte[] number = new byte[2];
        number[0] = (byte) 0b00000110;
        number[1] = (byte) 0b10001001;

        int result = VariableByte.decodeInteger(number);
        assertEquals(777, result);
    }

    @Test
    @DisplayName("Test encoding-decoding")
    void testEncodingDecoding() {
        Random r = new Random();
        int number = r.nextInt(MAX_VALUE) + 1;
        byte[] encoded = VariableByte.encodeInteger(number);
        assertEquals(number, VariableByte.decodeInteger(encoded));
    }
}