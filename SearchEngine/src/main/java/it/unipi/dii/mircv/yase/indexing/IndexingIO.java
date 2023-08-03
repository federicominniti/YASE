package it.unipi.dii.mircv.yase.indexing;

import it.unipi.dii.mircv.yase.compression.Unary;
import it.unipi.dii.mircv.yase.compression.VariableByte;
import it.unipi.dii.mircv.yase.structures.LexiconEntry;
import it.unipi.dii.mircv.yase.util.Configuration;
import it.unipi.dii.mircv.yase.structures.PostingList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class IndexingIO {
    private static final int SKIP_BLOCKS_THRESHOLD = Configuration.indexingSkipBlocksThreshold;
    private static RandomAccessFile rafTf = null;
    private static RandomAccessFile rafDocid = null;
    private static DataOutputStream batchWritesBufferTF = null;
    private static DataOutputStream batchWritesBufferDocid = null;
    private static int batchWritesCurrentOffsetTF = 0;
    private static int batchWritesCurrentOffsetDocid = 0;
    private static final String FILE_PREFIX = Configuration.invertedIndexPartialDir + Configuration.invertedIndexPartialPrefix;
    private static final HashMap<Integer, RandomAccessFile> openedPartialInvertedIndexes = new HashMap<>();
    private static final Logger logger = Logger.getLogger(IndexingIO.class);

    /**
     * Method used both to take the {@link PartialLexicon} of the analyzed block
     * and to write the inverted index on  disk.
     * @param block partial inverted index
     * @param blockId number used to identify the block
     * @return partial lexicon build from the block
     */
    public static List<LexiconEntry> writePartialIndexFromBlock(
            final HashMap<String, PostingList> block,
            int blockId
    ) {

        // compute the total number of postings
        int numPostings = 0;
        for (PostingList postingList : block.values()) {
            numPostings += postingList.getPostingsCounter();
        }

        List<LexiconEntry> partialLexicon = new ArrayList<>();

        // length of the block
        long bufferSize = (long) 2 * numPostings *Integer.BYTES + (long) block.size() * Integer.BYTES;

        try {
            // control if the directory exists
            checkDirectories(Configuration.invertedIndexPartialDir);
            // prepare the I/O with the current block
            RandomAccessFile raf = new RandomAccessFile(FILE_PREFIX + blockId, "rw");
            MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);

            // iterate over the entries of the block
            for (Map.Entry<String, PostingList> entry: block.entrySet()) {
                // skip the terms with empty string
                if (entry.getKey().equals(""))
                    continue;
                // retrieve information
                String term = entry.getKey();
                PostingList p = entry.getValue();
                // prepare the partial lexicon
                partialLexicon.add(new LexiconEntry(term, buffer.position()));
                // write the length of the posting list
                buffer.putInt(p.getPostingsCounter());
                int[] docids = p.getDocIds();
                int[] tfs = p.getTfs();
                for (int i = 0; i < p.getPostingsCounter(); i++) {
                    // write the couples <docid, TF>
                    buffer.putInt(docids[i]);
                    buffer.putInt(tfs[i]);
                }
            }

            buffer.force();
            raf.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        // sort the partial lexicon by alphanumeric order
        Collections.sort(partialLexicon, LexiconEntry::compareTo);
        return partialLexicon;
    }

    /**
     * Method used to read the content of a partial inverted index.
     * @param offset of the partial index
     * @param blockId number that identify the block
     * @return the {@link PostingList} inside the partial inverted index
     */
    public static PostingList readFromPartialIndexFile(final long offset, int blockId) {
        int[][] postings = null;
        try {
            RandomAccessFile raf;
            // check if the file of the partial index is already open
            if ((raf = openedPartialInvertedIndexes.get(blockId)) == null) {
                raf = new RandomAccessFile(FILE_PREFIX + blockId, "r");
                openedPartialInvertedIndexes.put(blockId, raf);
            }

            // move the pointer to the beginning of the posting list
            raf.seek(offset);
            DataInputStream reader = new DataInputStream(new BufferedInputStream(new FileInputStream(raf.getFD())));
            // read the length of the posting list
            int postingListLength = reader.readInt();

            postings = new int[2][postingListLength];
            int[] docIds = postings[0];
            int[] TFs = postings[1];
            // read all the couples <docid, TF>
            for (int i = 0; i < postingListLength; i++) {
                docIds[i] = reader.readInt();
                TFs[i] = reader.readInt();
            }

        } catch (IOException e) {
            logger.fatal(e.getMessage(), e);
        }

        return new PostingList(postings != null ? postings : new int[0][]);
    }

    /**
     * Method used to write a posting list on file, by taking in consideration also the {@link it.unipi.dii.mircv.yase.querying.SkippingBlocks}
     * information in order to achieve an easier encoding. The posting list is written on two files, on containing the
     * docids and the other the TFs.
     * @param postingList to write on the disk
     * @return the couple containing the offsets of the beginning of both files.
     *
     */
    public static Pair<Long, Long> writePostingList(PostingList postingList) throws IOException {
        // the concatenated bytes of the compressed TFs will be written here
        ByteArrayOutputStream compressedPostingsTf = new ByteArrayOutputStream();
        // the concatenated bytes of the compressed docids will be written here
        ByteArrayOutputStream compressedPostingsDocid = new ByteArrayOutputStream();


        long startingOffsetDocids = batchWritesCurrentOffsetDocid;
        long startingOffsetTFs = batchWritesCurrentOffsetTF;

        // the skip block contains information that will be useful during the posting list processing
        int skipBlocksSize;
        int blockSize = -1;
        // if the number of postings do not reach the threshold, we need just a skip block
        if (postingList.getPostingsCounter() < SKIP_BLOCKS_THRESHOLD)
            skipBlocksSize = 1;
        // otherwise we have a skip block for each group of postings, containing the information of the group
        else {
            // number of postings for group
            blockSize = (int) Math.ceil(Math.sqrt(postingList.getPostingsCounter()));
            skipBlocksSize = (int)Math.ceil((double) postingList.getPostingsCounter()/blockSize);
        }

        // reserve the first 4 bytes of the docids file to write the length of the skip pointers info
        // we don't need this in the TFs file because it will be used also with the docids file
        batchWritesBufferDocid.writeInt(skipBlocksSize);
        batchWritesCurrentOffsetDocid += Integer.BYTES;

        int[] tfs = postingList.getTfs();
        int[] docids = postingList.getDocIds();

        int lastDocid = 0;
        // case of number of postings < threshold
        if (skipBlocksSize == 1) {
            // iterate over each posting
            for (int index = 0; index < postingList.getPostingsCounter(); index++) {
                // write the docids by computing the difference from the previous in order of save storage
                // This is possible because docids inside a posting list increase monotonically
                // ex [3, 15, 48] -> [3, 12, 33]
                compressedPostingsDocid.write(VariableByte.encodeInteger(docids[index] - lastDocid));
                lastDocid = docids[index];
            }

            // write unary encoded TFs
            compressedPostingsTf.write(Unary.encodeIntegerArray(tfs));

            //write to file the length in bytes of the compressed TFs
            batchWritesBufferTF.writeInt(compressedPostingsTf.size());
            batchWritesCurrentOffsetTF += Integer.BYTES;
            //write to file the max docid
            batchWritesBufferDocid.writeInt(docids[postingList.getPostingsCounter()-1]);
            batchWritesCurrentOffsetDocid += Integer.BYTES;
            //write to file the length in bytes of the compressed docids
            batchWritesBufferDocid.writeInt(compressedPostingsDocid.size());
            batchWritesCurrentOffsetDocid += Integer.BYTES;

        // case of number of postings >= threshold
        } else {
            int startIndex = 0;
            int lastCompressedBlockSize = 0;

            // iterate over the posting list
            for (int index = 0; index < postingList.getPostingsCounter(); index++) {
                // write compressed docids
                compressedPostingsDocid.write(VariableByte.encodeInteger(docids[index] - lastDocid));
                lastDocid = docids[index];

                // if we have reached the size of a block or the posting list has been completely traversed
                if ((index > 0 && index % blockSize == blockSize-1) || index == postingList.getPostingsCounter()-1) {
                    // encode a block of compressed TFs
                    byte[] compressedTFBlock = Unary.encodeIntegerArray(Arrays.copyOfRange(tfs, startIndex, index+1));

                    // write the encoded block of TFs
                    compressedPostingsTf.write(compressedTFBlock);
                    // keep track of where to start encoding the TFs next time
                    startIndex = index + 1;

                    // write to file skip pointers information
                    batchWritesBufferTF.writeInt(compressedTFBlock.length);
                    batchWritesCurrentOffsetTF += Integer.BYTES;
                    batchWritesBufferDocid.writeInt(docids[index]);
                    batchWritesCurrentOffsetDocid += Integer.BYTES;
                    batchWritesBufferDocid.writeInt(compressedPostingsDocid.size() - lastCompressedBlockSize);
                    batchWritesCurrentOffsetDocid += Integer.BYTES;
                    // update last compressed block size because we want only the lengths, not the total bytes to skip
                    lastCompressedBlockSize = compressedPostingsDocid.size();
                }
            }
        }

        //finally write the encoded TFs and docids
        byte[] compressedTFsBytes = compressedPostingsTf.toByteArray();
        batchWritesBufferTF.write(compressedTFsBytes);
        batchWritesCurrentOffsetTF += compressedTFsBytes.length;

        byte[] compressedDocidsBytes = compressedPostingsDocid.toByteArray();
        batchWritesBufferDocid.write(compressedDocidsBytes);
        batchWritesCurrentOffsetDocid += compressedDocidsBytes.length;

        // return the offset of the beginning of the two files
        return Pair.of(startingOffsetDocids, startingOffsetTFs);
    }

    /**
     * Method used to prepare the {@link DataOutputStream} of the two files, one for the docids and the
     * other for the TFs
     * @param fileNameTFs name of the file containing the TFs
     * @param fileNameDocids name of the file containing the docids
     */
    public static void prepareForBatchWrites(String fileNameTFs, String fileNameDocids) {
        try {
            // if the pointers were already init, they will be clean.
            if (rafTf != null)
                rafTf.close();
            if (rafDocid != null)
                rafDocid.close();
            checkDirectories(Configuration.finalDir);
            // prepare the Data Output Stream
            rafTf = new RandomAccessFile(fileNameTFs, "rw");
            rafDocid = new RandomAccessFile(fileNameDocids, "rw");
            batchWritesBufferTF = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(rafTf.getFD())));
            batchWritesBufferDocid = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(rafDocid.getFD())));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Method used to close all the files opened during the indexing.
     */
    public static void closeFiles() {
        try {
            if (batchWritesBufferDocid != null)
                batchWritesBufferDocid.close();
            if (batchWritesBufferTF != null)
                batchWritesBufferTF.close();

            if (rafTf != null) {
                if (rafTf.getChannel().isOpen())
                    rafTf.close();
            }

            if (rafDocid != null) {
                if (rafDocid.getChannel().isOpen())
                    rafDocid.close();
            }

            for (RandomAccessFile raf: openedPartialInvertedIndexes.values()) {
                raf.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        rafTf = null;
        rafDocid = null;
    }

    /**
     * Method used to create a directory where we will store the partial structures.
     * @param path of the directory
     */
    private static void checkDirectories(String path){
        try {
            Path directoriesPath = Paths.get(path);
            Files.createDirectories(directoriesPath);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
