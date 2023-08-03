package it.unipi.dii.mircv.yase.structures;

import java.util.Arrays;

/**
 * A simple class to handle some operations involving posting lists during the indexing phase
 */
public class PostingList {
    // array of postings: the row 0 contains the docids, the row 1 contains the TFs
    private int[][] postings;

    // number of postings in the list
    private int postingsCounter;

    /**
     * Initializes the list size to 1 and inserts the first docid with tf=1
     * @param docId
     */
    public PostingList (final int docId) {
        postings = new int[2][1];
        postings[0][0] = docId;
        postings[1][0] = 1;
        postingsCounter = 1;
    }

    /**
     * Initializes the posting list with an array of postings
     * @param postings
     */
    public PostingList (final int[][] postings) {
        this.postings = postings;
        postingsCounter = postings[0].length;
    }

    /**
     * Initializes the posting list with an array of docids and an array of tfs
     * @param docids
     * @param tfs
     */
    public PostingList (int[] docids, int[] tfs) {
        postings = new int[2][docids.length];
        postings[0] = docids;
        postings[1] = tfs;
        postingsCounter = docids.length;
    }

    /**
     * Method used during the index construction.
     * If the docid passed is not in the posting list, it is added (eventually the size of the list is increased)
     * If the docId is already present the corresponding TF is incremented by 1
     * @param docId
     */
    public void updatePosting(final int docId){
        int position = indexOf(docId);
        if (position >= 0) {
            postings[1][position]++;
            return;
        }

        addEntry(docId, 1);
    }

    /**
     * Adds a (docid,tf) entry to the posting list, doubling its size if necessary
     * @param docId
     * @param TF
     */
    public void addEntry(Integer docId, Integer TF) {
        if (postingsCounter == postings[0].length)
            doubleSize();

        postings[0][postingsCounter] = docId;
        postings[1][postingsCounter] = TF;

        postingsCounter++;
    }

    /**
     * Method to double the size of the posting list. During indexing, instead
     * of incrementing the size of the posting list by 1 every time, we double its size
     * keeping track of the real number of postings contained in the list
     */
    public void doubleSize(){
        int currentSize = postings[0].length;
        postings[0] = Arrays.copyOf(postings[0], currentSize*2);
        postings[1] = Arrays.copyOf(postings[1], currentSize*2);
    }

    /**
     * Method to merge two posting lists; the passed one is attached at the end of the other
     * @param toAdd
     */
    public void mergePostings(final PostingList toAdd){
        int newLength = postingsCounter + toAdd.getPostingsCounter();
        postings[0] = Arrays.copyOf(postings[0], newLength);
        postings[1] = Arrays.copyOf(postings[1], newLength);
        System.arraycopy(toAdd.getDocIds(), 0, postings[0], postingsCounter, toAdd.getPostingsCounter());
        System.arraycopy(toAdd.getTfs(), 0, postings[1], postingsCounter, toAdd.getPostingsCounter());
        postingsCounter += toAdd.getPostingsCounter();
    }

    /**
     * Returns the true length of this posting list
     */
    public int getPostingsCounter(){
        return postingsCounter;
    }

    /**
     * Returns the docids in the posting list
     */
    public int[] getDocIds(){
        return postings[0];
    }

    /**
     * Returns the TFs of the posting list
     */
    public int[] getTfs(){
        return postings[1];
    }

    /**
     * Returns the index in the posting list associated to a docid
     * @param docId
     */
    public int indexOf(int docId){
        return Arrays.binarySearch(postings[0], 0, postingsCounter, docId);
    }
}
