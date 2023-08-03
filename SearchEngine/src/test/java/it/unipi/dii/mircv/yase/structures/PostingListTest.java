package it.unipi.dii.mircv.yase.structures;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PostingListTest {

    @Test
    @DisplayName("Testing doubling size on multiple insertions")
    void testDoubleSize() {
        PostingList p = new PostingList(0);
        for (int i = 0; i < 1000; i++) {
            p.addEntry(i, 1);
        }

        assert(p.getDocIds().length == 1024);
        assert(p.getPostingsCounter() == 1001);
    }

    @Test
    @DisplayName("Testing merge")
    void testMerge() {
        PostingList first = new PostingList(0);
        PostingList second = new PostingList(1000);
        for (int i = 1; i < 1000; i++) {
            first.addEntry(i, 1);
            second.addEntry(i+1000, 1);
        }

        first.mergePostings(second);
        assert(first.getDocIds().length == 2000);
        for (int i = 1; i < 2000; i++) {
            assert(first.getDocIds()[i] == i);
        }
    }
}
