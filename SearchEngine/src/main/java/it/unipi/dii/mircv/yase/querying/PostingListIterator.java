package it.unipi.dii.mircv.yase.querying;

import com.google.common.primitives.Bytes;
import it.unipi.dii.mircv.yase.compression.Unary;
import it.unipi.dii.mircv.yase.compression.VariableByte;
import it.unipi.dii.mircv.yase.indexing.DocumentIndex;
import it.unipi.dii.mircv.yase.structures.CollectionStatistics;
import it.unipi.dii.mircv.yase.util.Configuration;
import it.unipi.dii.mircv.yase.structures.LexiconEntry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Implementation of an iterator of a posting list
 */
public class PostingListIterator {
    // Reader of the next encoded doc id (gap) in the DocIds' index file
    private RandomAccessFile rafDocids;
    private DataInputStream docidsReader;

    // Reader of the next encoded term frequency in the term frequencies' index file
    private RandomAccessFile rafTF;
    private DataInputStream TFReader;

    // Skipping block of docids and tf
    private SkippingBlocks skippingBlocks;

    // Flag to indicate if the posting list is ended or there are other postings available to be read
    private boolean hasNext;

    // First byte of the next docid(gap) to decompress with VariableByte; it is also the last byte analyzed during
    // the last decompression
    private Byte currentByteVB;

    // Current docid of the posting list iterator
    private Integer currentDocid;

    // Current term frequency of the posting list iterator
    private Integer currentTF;

    // Bits of the current encoded (Unary) tfs' block
    private BitSet currentTFsBlock;

    // Current position of the pointer to the start of the next bit of the encoded term frequency
    private int currentTFPositionInBlock;

    // Current index of encoded tfs' block
    private int currentTFBlockIndex;

    // Collection statistics previously created and stored on the disk
    private static CollectionStatistics collectionStatistics;

    // Document index previously created and stored on the disk
    private static DocumentIndex documentIndex;

    // Number of postings in the posting list
    private final int postingListSize;

    private final double LOG2 = Math.log(2);
    private final static Logger logger = Logger.getLogger(PostingListIterator.class);

    public PostingListIterator(LexiconEntry lexiconEntry, DocumentIndex docIndex) {
        collectionStatistics = CollectionStatistics.getInstance();
        documentIndex = docIndex;
        postingListSize = lexiconEntry.getDocfreq();

        try {
            rafDocids = new RandomAccessFile(Configuration.finalDir + Configuration.invertedIndexDocidsName, "r");
            rafTF = new RandomAccessFile(Configuration.finalDir + Configuration.invertedIndexTFsName, "r");

            rafDocids.seek(lexiconEntry.getOffsetDocid());
            rafTF.seek(lexiconEntry.getOffsetTermFrequency());

            skippingBlocks = new SkippingBlocks(rafDocids, rafTF);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        currentTFBlockIndex = 0;
        loadReaders();
        loadNextTFBlock();

        hasNext = true;
        currentByteVB = null;
        currentDocid = 0;
        currentTF = 0;
    }

    /**
     * Method to move the posting list iterator to the next posting
     * @return
     */
    public Pair<Integer, Integer> next() {
        if (!hasNext) {
            currentDocid = null;
            currentTF = null;
            return null;
        }

        // Variable byte for docid (gap) decompression
        byte tmp;
        List<Byte> buffer = new ArrayList<>();
        try {
            if (currentByteVB == null) {
                tmp = docidsReader.readByte();
            } else {
                tmp = currentByteVB;
            }
            buffer.add(tmp);
            do {
                try {
                    tmp = docidsReader.readByte();
                } catch (EOFException EOF) {
                    break;
                }
                if (!VariableByte.isMSBUnset(tmp)) {
                    buffer.add(tmp);
                }
                currentByteVB = tmp;
            } while (!VariableByte.isMSBUnset(tmp));
            // We scan all the bytes until we will find the first byte of the next encoded docid that we store
            // for the next invocation of the next() method
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        // We reconstruct the docid with the decoded gap
        currentDocid += VariableByte.decodeInteger(Bytes.toArray(buffer));

        if (currentDocid == skippingBlocks.getMaxDocid()) {
            // The current doc id is the last doc id of the posting list
            hasNext = false;
        }

        buffer.clear();

        // Unary for term frequencies decompression
        currentTF = Unary.getIntegerFromBuffer(currentTFsBlock, currentTFPositionInBlock);
        currentTFPositionInBlock += currentTF;

        if (currentDocid == skippingBlocks.getMaxDocidOfBlock(currentTFBlockIndex) && hasNext) {
            // The current doc id is the last doc id of the block, and there are other blocks to read
            currentTFBlockIndex++;
            loadNextTFBlock();
        }

        return Pair.of(currentDocid, currentTF);
    }

    /**
     * Method to move the iterator forward to the next posting with a doc id greater than or equal to the doc id passed
     * @param minDocid
     * @return
     */
    public Pair<Integer, Integer> nextGeq(int minDocid){
        if (minDocid > skippingBlocks.getMaxDocid()) {
            // The passed doc id is greater than the maximum doc id of the posting list
            currentDocid = null;
            currentTF = null;
            return null;
        }

        // Starting from the current block we go to search the block that contains the doc id greater than or equal to
        // the doc id passed
        int startingBlockIndex = currentTFBlockIndex;
        currentTFBlockIndex = skippingBlocks.binarySearchGEQ(currentTFBlockIndex, minDocid);

        try {
            if (startingBlockIndex != currentTFBlockIndex) {
                // We move the pointer of doc ids (gap) and term frequencies at the beginning of the correct block
                long newBlockOffsetDocids = skippingBlocks.getBlockOffsetDocids(currentTFBlockIndex);
                long newBlockOffsetTFs = skippingBlocks.getBlockOffsetTFs(currentTFBlockIndex);
                rafDocids.seek(newBlockOffsetDocids);
                rafTF.seek(newBlockOffsetTFs);

                // We update all the variables in order to read the next posting
                loadReaders();
                loadNextTFBlock();
                currentDocid = skippingBlocks.getMaxDocidOfBlock(currentTFBlockIndex-1);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        // We read and element of the posting list until we reach a doc id that is greater or equal of the passed doc id
        while (currentDocid != null && currentDocid < minDocid)
            next();

        return Pair.of(currentDocid, currentTF);
    }

    /**
     * Getter of the current posting of the iterator
     * @return
     */
    public Pair<Integer, Integer> getCurrentPosting() {
        return Pair.of(currentDocid, currentTF);
    }

    /**
     * Method to load buffered readers that start that starts where their pointers are located, for both the doc id's index
     * file and the term frequency's index file.
     */
    private void loadReaders() {
        try {
            docidsReader = new DataInputStream(new BufferedInputStream(new FileInputStream(rafDocids.getFD())));
            TFReader = new DataInputStream(new BufferedInputStream(new FileInputStream(rafTF.getFD())));
            currentByteVB = null;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Load a new term frequency block considering the position of the pointer in the term frequency index file
     */
    private void loadNextTFBlock(){
        byte[] firstTFBlockBuffer = new byte[skippingBlocks.getTFBlockLength(currentTFBlockIndex)];

        try {
            TFReader.read(firstTFBlockBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Update variable coherently in order to read the next term frequency correctly
        currentTFsBlock = BitSet.valueOf(firstTFBlockBuffer);
        currentTFPositionInBlock = 0;
    }

    /**
     * Method to compute BM25 score of the current document with respect to the associated term of the posting list iterator
     * @return
     */
    private double computeBM25Score(){
        double avgDocLen = collectionStatistics.getAverageDocumentLength();
        double b = Configuration.bm25B;
        double k1 = Configuration.bm25K1;
        int dl = documentIndex.getAtGEQIndex(currentDocid).getDocLen();
        double K = k1 * ((1 - b) + b * dl / avgDocLen);
        double idf = Math.log((collectionStatistics.getNumberOfDocuments() - postingListSize + 0.5d)
                / (postingListSize + 0.5d))
                / LOG2;
        return idf * ((k1 + 1d) * currentTF / (K + currentTF));
    }

    /**
     * Method to compute TFIDF score of the current document with respect to the associated term of the posting list iterator
     * @return
     */
    private double computeTFIDFScore(){
        double tf = 1 + Math.log(currentTF)/LOG2;
        double idf = Math.log((collectionStatistics.getNumberOfDocuments() - postingListSize + 0.5d)
                / (postingListSize + 0.5d))
                / LOG2;
        return tf*idf;
    }

    /**
     * Method to return a score depending on the specific passed scoring function
     * @param scoringFunction
     * @return
     */
    public double getScore(int scoringFunction) {
        if (scoringFunction == Configuration.TFIDF) {
            return computeTFIDFScore();
        }
        if (scoringFunction == Configuration.BM25) {
            return computeBM25Score();
        }
        return -1;
    }

    /**
     * Method to close the index files used
     */
    public void closeFiles(){
        try {
            rafTF.close();
            rafDocids.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
