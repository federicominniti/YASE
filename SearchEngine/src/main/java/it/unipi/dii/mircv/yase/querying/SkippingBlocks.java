package it.unipi.dii.mircv.yase.querying;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that maintain and handle, for a specific posting list, all its skipping block (for both the docids index file
 * and term frequency index file)
 */
public class SkippingBlocks {
    // List of maximum doc ids for each block of doc ids (gap)
    private List<Integer> maxDocids;

    // List of the beginning offset for each block of doc ids (gap)
    private List<Long> blockOffsetsDocids;

    // List of the beginning offset for each block of term frequency
    private List<Long> blockOffsetsTFs;

    // List of maximum block length (bytes) for each block of term frequencies
    private List<Integer> blockLengthsTFs;

    private final static Logger logger = Logger.getLogger(SkippingBlocks.class);

    public SkippingBlocks(RandomAccessFile docidsReader, RandomAccessFile tfReader) {
        maxDocids = new ArrayList<>();
        blockLengthsTFs = new ArrayList<>();
        blockOffsetsDocids = new ArrayList<>();
        blockOffsetsTFs = new ArrayList<>();

        // We read from the doc ids index the number of block in which the posting list is divided
        int skipBlocksSize = 0;
        try {
            skipBlocksSize = docidsReader.readInt();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        int offsetSumDocids = 0;
        int offsetSumTFs = 0;

        // We go to read the skip block info for each block
        for (int i = 0; i < skipBlocksSize; i++) {

            // We add for each block the sum of the sizes of the previous blocks, in order to create, for each block, its
            // offset starting from 0
            blockOffsetsDocids.add((long) offsetSumDocids);
            blockOffsetsTFs.add((long) offsetSumTFs);

            try {
                maxDocids.add(docidsReader.readInt());
                offsetSumDocids += docidsReader.readInt();
                int tfBlockLength = tfReader.readInt();
                offsetSumTFs += tfBlockLength;
                blockLengthsTFs.add(tfBlockLength);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // Now we have the list of offset (for both the docids and tfs) starting from 0, and we need to align them to
        // the beginning of the posting list (after the end of the skipping blocks). When we read the last skipping of the
        // posting list, we go to sum the offset of the posting list's pointer to all the 0-based offset previously calculated
        // (As posting list pointer we mean separately the two pointer of term frequencies index file and doc ids index file)
        for (int i = 0; i < skipBlocksSize; i++) {
            long blockOffsetDocids = blockOffsetsDocids.get(i);
            long blockOffsetTFs = blockOffsetsTFs.get(i);
            try {
                blockOffsetsDocids.set(i, blockOffsetDocids + docidsReader.getFilePointer());
                blockOffsetsTFs.set(i, blockOffsetTFs + tfReader.getFilePointer());
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Method that implement a binary search in the skipping blocks in order to search a block that contains a doc id
     * that is greater or equal to the doc id passed as parameter
     * @param start
     * @param minDocid
     * @return
     */
    public int binarySearchGEQ(int start, int minDocid) {
        int end = maxDocids.size()-1;
        while (start < end) {
            int mid = start + (end - start)/2;
            if (maxDocids.get(mid) < minDocid) {
                start = mid + 1;
            } else
                end = mid;
        }

        return end;
    }

    /**
     * Getter of the maximum doc id of the considered posting list
     * @return
     */
    public int getMaxDocid() {
        return maxDocids.get(maxDocids.size() - 1);
    }

    /**
     * Getter of the tf block's length given an index
     * @param index
     * @return
     */
    public int getTFBlockLength(int index) {
        return blockLengthsTFs.get(index);
    }

    /**
     * Getter of the maximum doc id of the block of the given index
     * @param index
     * @return
     */
    public int getMaxDocidOfBlock(int index) {
        return maxDocids.get(index);
    }

    /**
     * Getter of the doc id block's offset given an index
     * @param index
     * @return
     */
    public long getBlockOffsetDocids(int index) {
        return blockOffsetsDocids.get(index);
    }

    /**
     * Getter of the tf block's offset given an index
     * @param index
     * @return
     */
    public long getBlockOffsetTFs(int index) {
        return blockOffsetsTFs.get(index);
    }

    /**
     * Getter of the number of skipping blocks
     * @return
     */
    public int getSize() { return maxDocids.size(); }
}
