
package it.unipi.dii.mircv.yase.indexing;

import it.unipi.dii.mircv.yase.structures.LexiconEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LexiconTest {
    Lexicon test;
    int size = 10000;
    @BeforeEach
    void setUp() throws IOException {
        File f = new File("final_lexicon_test");
        if (f.exists())
            f.delete();

        test = new Lexicon(".", "htable_test", size, 10000, 1);
    }

    @Test
    @DisplayName("Testing HashTable on disk")
    void testPutAllMMap() throws IOException {

        List<LexiconEntry> list = new ArrayList<>();
        Random random = new Random();
        int leftLimit = 48;
        int rightLimit = 122;
        int targetStringLength = 10;
        //generate random strings with fixed length == 10
        for (int j = 0; j < size; j++) {
            String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                    .limit(targetStringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();

            list.add(new LexiconEntry(generatedString+j, j, j, j, j, j));
        }

        test.putAllMMap(list);

        for (int i = 0; i < list.size(); i++) {
            test.get(list.get(i).getID());
        }

    }
}
