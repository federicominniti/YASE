package it.unipi.dii.mircv.yase.indexing;

import it.unipi.dii.mircv.yase.structures.LexiconEntry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class IndexFileControllerTest {

//    @Test
//    @DisplayName("Test R/W of a compressed PostingList block")
//    void testWritePostingList() throws IOException {
//        //test works but it's terrible to look at
//
//        indexFileController.writePostingList(postingList);
//
//        int searchedDocid = 0;
//        int tf = indexFileController.nextGEQ(searchedDocid, 0, 0);
//        if (tf == 0) {
//            System.out.println("searched docid is not present, try changing 'searchedDocid'");
//            System.exit(0);
//        }
//
//        List<Integer> originalDocids = Ints.asList(postingList.getDocIds());
//        List<Integer> originalTfs = Ints.asList(postingList.getTfs());
//        if (size < 1024) {
//            List<Integer> docids = Ints.asList(p.getDocIds());
//            List<Integer> tfs = Ints.asList(p.getTfs());
//            assertEquals(originalDocids, docids);
//            assertEquals(originalTfs, tfs);
//
//        } else {
//            final int blockSize = (int) Math.ceil(Math.sqrt(size));
//            final int searchedBlockStart = (searchedDocid/100)/blockSize * blockSize;
//            int indexOfStart = originalDocids.indexOf(searchedBlockStart*100);
//            int indexOfEnd = indexOfStart + blockSize;
//            if (indexOfEnd > size)
//                indexOfEnd = size;
//            List<Integer> expectedDocids = originalDocids.subList(indexOfStart, indexOfEnd);
//            List<Integer> expectedTFs = originalTfs.subList(indexOfStart, indexOfEnd);
//            List<Integer> docids = Ints.asList(p.getDocIds());
//            List<Integer> tfs = Ints.asList(p.getTfs());
//            assertEquals(expectedDocids, docids);
//            assertEquals(expectedTFs, tfs);
//            assert(docids.contains(searchedDocid));
//            assert(docids.size() == blockSize);
//            assertEquals(docids.size(), tfs.size());
//        }
//    }

    //@Test
    //@DisplayName("test write posting list")
    //void testWrite() throws IOException {
    //    Lexicon lex = new Lexicon("../final_lexicon", 30, 0, 1);
    //    LexiconEntry e = lex.get("manhattan");
    //    System.out.println(e);
    //    int tf = IndexingIO.nextGEQ(400, 0, 0);
    //}
}