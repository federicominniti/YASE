package it.unipi.dii.mircv.yase.indexing;

import it.unipi.dii.mircv.yase.structures.DocumentDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DocumentIndexTest {
    DocumentIndex index;
    @BeforeEach
    void setUp() {
        File f = new File("doc_index_test");
        if (f.exists())
            f.delete();

        index = new DocumentIndex(".", "doc_index_test", 20);
        index.openFile(true);
    }
    @Test
    @DisplayName("putAllMMap with List")
    void testPutAllMMapList() throws IOException {
        List<DocumentDescriptor> list = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            list.add(new DocumentDescriptor(i, "doc"+i, i*10));
        }

        index.openFile(true);
        index.putAllMMap(list);
        DocumentDescriptor e = new DocumentDescriptor(0, "doc"+0, 0);
        assertEquals(0, e.compareTo(index.get(0)));
        e = new DocumentDescriptor(9999, "doc"+0, 0);
        assertEquals(0, e.compareTo(index.get(9999)));
        e = new DocumentDescriptor(3455, "doc"+0, 0);
        assertEquals(0, e.compareTo(index.get(3455)));
    }
}