package it.unipi.dii.mircv.yase.compression;
import com.google.common.primitives.Bytes;
import java.util.ArrayList;
import java.util.List;

/**
 *  A class for <a href="https://nlp.stanford.edu/IR-book/html/htmledition/variable-byte-codes-1.html">Variable Byte</a>
 *  compression/decompression of arrays of integers.
 */
public class VariableByte {
    /**
     * Method to encode an integer as an array of bytes.
     * @param n integer to encode
     * @return array of bytes
     */
    public static byte[] encodeInteger(int n) {
        List<Byte> codeWord = new ArrayList<>();

        // special case
        if (n == 0) {
            codeWord.add((byte)0);
            return new byte[]{0};
        }

        byte tempByte;
        while (true) {
            // we do not need other bytes
            if(n < 128) {
                // just convert to byte, be
                codeWord.add(0, (byte)n);
                break;
            // a byte is not enough to represent the integer
            }else {
                // set the most significant bit to 1
                tempByte = (byte) ((n & 0b01111111) | 0b10000000);
                codeWord.add(0, tempByte);
                // shift the integer
                n = n >> 7;
            }
        }
        // convert the List to an array
        return Bytes.toArray(codeWord);
    }

    /**
     * Decode an array of bytes in an integer.
     * @param codeWord encoded array of bytes
     * @return decoded integer
     */
    public static int decodeInteger(byte[] codeWord) {
        int result = 0;
        int counter = 0;
        for (int i = codeWord.length - 1; i >= 0; i--) {
            byte temp = codeWord[i];
            // mask the most significant bit
            temp = (byte) (temp & 0b01111111);
            // it is equivalent to a multiplication and a sum
            result = (temp << (7 * counter)) | result;
            counter++;
        }

        return result;
    }

    /**
     * Check if the Control Bit is unset.
     * @param byteToCheck the byte to check
     * @return result
     */
    public static boolean isMSBUnset(byte byteToCheck){
        return ((byteToCheck & 0b10000000) == 0b00000000);
    }
}
