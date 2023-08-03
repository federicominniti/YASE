package it.unipi.dii.mircv.yase.structures;

import it.unipi.dii.mircv.yase.util.Configuration;

/**
 * A simple class a term of the lexicon
 */
public class LexiconEntry implements Comparable<LexiconEntry>, IdentifiableObject<String> {
    private String term;
    private int documentFrequency;
    private long offsetDocid;
    private long offsetTermFrequency;
    private double upperBoundTFIDF;
    private double upperBoundBM25;

    /**
     * Creates a new lexicon entry for the partial lexicon
     * @param term
     * @param offsetDocid
     */
    public LexiconEntry(final String term, final long offsetDocid) {
        this.term = term;
        this.offsetDocid = offsetDocid;
    }

    /**
     * Creates a new lexicon entry for the final lexicon
     * @param term
     * @param documentFrequency
     * @param offsetDocid
     * @param offsetTermFrequency
     * @param maxTFIDFWeight
     * @param maxBM25Weight
     */
    public LexiconEntry(final String term, final int documentFrequency, final long offsetDocid, final long offsetTermFrequency,
                        final double maxTFIDFWeight, final double maxBM25Weight) {
        this.term = term;
        this.documentFrequency = documentFrequency;
        this.offsetDocid = offsetDocid;
        this.offsetTermFrequency = offsetTermFrequency;
        this.upperBoundTFIDF = maxTFIDFWeight;
        this.upperBoundBM25 = maxBM25Weight;
    }

    public String getTerm() {
        return term.replaceAll("\\s", "");
    }

    public int getDocfreq() {
        return documentFrequency;
    }

    public long getOffsetDocid() {
        return offsetDocid;
    }

    public long getOffsetTermFrequency() {
        return offsetTermFrequency;
    }

    /**
     * Returns the term upper bound with respect to a certain scoring function
     * @param scoringFunction
     */
    public double getUpperBound(int scoringFunction) {
        if (scoringFunction == Configuration.TFIDF) {
            return upperBoundTFIDF;
        }
        if (scoringFunction == Configuration.BM25) {
            return upperBoundBM25;
        }
        return -1;
    }

    public Double getUpperBoundTFIDF() {
        return upperBoundTFIDF;
    }

    public Double getUpperBoundBM25() {
        return upperBoundBM25;
    }

    @Override
    public String toString() {
        return "LexiconEntry{" +
                "term='" + term + '\'' +
                ", docfreq=" + documentFrequency +
                ", offsetDocid=" + offsetDocid +
                ", offsetTermFrequency=" + offsetTermFrequency +
                ", upperBoundTFIDF=" + upperBoundTFIDF +
                ", upperBoundBM25=" + upperBoundBM25 +
                '}';
    }

    /**
     * Compares two entries of the lexicon by increasing lexicographical order
     * @param e the object to be compared.
     */
    public int compareTo(final LexiconEntry e) {
        return getTerm().compareTo(e.getTerm());
    }

    @Override
    public String getID() {
        return getTerm();
    }
}
