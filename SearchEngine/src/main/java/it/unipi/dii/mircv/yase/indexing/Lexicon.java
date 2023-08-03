package it.unipi.dii.mircv.yase.indexing;

import it.unipi.dii.mircv.yase.structures.HashTableFile;
import it.unipi.dii.mircv.yase.structures.LexiconEntry;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *  Class that implements the vocabulary of the search engine by extending {@link HashTableFile}.
 *  In this way the Lexicon can leverage the quick lookup time of a <a href="https://en.wikipedia.org/wiki/Hash_table">hash table</a>
 */
public class Lexicon extends HashTableFile<String, LexiconEntry> {
    private final static Logger logger = Logger.getLogger(Lexicon.class);

    /**
     * Constructor of the {@link Lexicon}
     * @param path to the directory of the lexicon.
     * @param fileName name of the file where lexicon information are stored.
     * @param maxStringLength max length of term.
     * @param maxNumObjects num of entries of the lexicon.
     * @param cacheSize size of the cache on top of the lexicon.
     */
    public Lexicon(String path, String fileName, final int maxStringLength, final int maxNumObjects, final int cacheSize) {
        // invoke the constructor of the HashTableFile class
        super(path, fileName, maxStringLength, maxNumObjects, cacheSize);

        // if we do not know the number of objects, we compute it
        if (maxNumObjects == 0) {
            openFile(false);
            try {
                SIZE = filePointer.length() / ENTRY_SIZE;
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Serializes a single LexiconEntry information inside the {@link MappedByteBuffer}
     * @param entry the entry to serialize
     * @param buffer the buffer used for memory-mapping
     */
    protected void writeEntry(final LexiconEntry entry, final MappedByteBuffer buffer) {
        //add spaces since the term does not reach the max string length
        String fixedSizeTerm = String.format("%1$"+maxStringLength+ "s", entry.getTerm());

        // write the term
        buffer.put(fixedSizeTerm.getBytes(StandardCharsets.US_ASCII));
        // write the offset to docids
        buffer.putInt((int)entry.getOffsetDocid());
        // write the offset to TFs
        buffer.putInt((int)entry.getOffsetTermFrequency());
        // write the document frequency (posting count)
        buffer.putInt(entry.getDocfreq());
        // write the upper bound for TF-IDF
        buffer.putFloat(entry.getUpperBoundTFIDF().floatValue());
        // write the upper bound for BM25
        buffer.putFloat(entry.getUpperBoundBM25().floatValue());
    }

    /**
     * Returns the fixed size of the LexiconEntry, considering the specified max string length.
     *
     * @return size.
     */
    @Override
    protected int getEntrySize(int maxStringLength) {
        // max string length + docids offset + TFs offset + DF + 2*Upper bound
        return maxStringLength + 3*Integer.BYTES + 2*Float.BYTES;
    }

    /**
     * Reads a single LexiconEntry using a RandomAccessFile object already at the correct offset
     * thanks to the hash table logic.
     * @return entry
     */
    @Override
    protected LexiconEntry readEntry() {
        // buffer to strore the term
        byte[] termBuffer = new byte[maxStringLength];
        // buffer to store the whole entry
        byte[] buffer = new byte[ENTRY_SIZE];
        try {
            filePointer.readFully(buffer);
            // there is no entry
            if (buffer[0] == 0)
                return null;
            // read the term
            ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer);
            DataInputStream dis = new DataInputStream(inputStream);
            dis.read(termBuffer);
            String term = new String(termBuffer, StandardCharsets.US_ASCII);
            // read docids offset
            long docidOffset = dis.readInt();
            // read TFs offset
            long tfOffset = dis.readInt();
            // read DF
            int docFreq = dis.readInt();
            // read upper bounds
            double maxTFIDFWeight = dis.readFloat();
            double maxBM25Weight = dis.readFloat();

            return new LexiconEntry(term, docFreq, docidOffset, tfOffset, maxTFIDFWeight, maxBM25Weight);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }
}
