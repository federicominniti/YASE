package it.unipi.dii.mircv.yase.querying;

import it.unipi.dii.mircv.yase.indexing.DocumentIndex;
import it.unipi.dii.mircv.yase.indexing.Lexicon;
import it.unipi.dii.mircv.yase.structures.DocumentDescriptor;
import it.unipi.dii.mircv.yase.structures.LexiconEntry;
import it.unipi.dii.mircv.yase.util.Configuration;
import it.unipi.dii.mircv.yase.util.preprocessing.TextPreprocessing;
import org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.util.*;

/**
 * Class to process and execute query given: the query, a scoring function and the type of query
 * we want to execute
 */
public class QueryProcessor {
    //lexicon previously created and stored on the disk
    private static final Lexicon lexicon;
    //document index previously created and stored on the disk
    private static final DocumentIndex documentIndex;

    //time variables in order to measure the time to execute the query
    private static long initTime = 0;
    private static long lastQueryTime = 0;

    static{
        lexicon = new Lexicon(Configuration.finalDir, Configuration.lexiconFinalName, Configuration.termLength, 0,
                Configuration.lexiconCache);
        documentIndex = new DocumentIndex(Configuration.finalDir, Configuration.documentIndexFinalName, Configuration.docnoLength);
        documentIndex.openFile(true);
    }

    /**
     * Method to preprocess the query and prepare all the data structure in order execute
     * the query in a disjunctive mode, with the DAAT algorithm.
     * @param query
     * @param scoringFunction
     * @return
     * @throws IOException
     */
    public static List<Pair<DocumentDescriptor, Double>> searchWithDAAT(String query, int scoringFunction) throws IOException {
        documentIndex.prepareToAnswerQuery();
        List<PostingListIterator> postingListIterators = new ArrayList<>();

        // Query preprocessing
        List<LexiconEntry> queryTerms = queryPreprocessing(query, false);
        if (queryTerms == null) {
            lastQueryTime = 0;
            return null;
        }


        // Posting list iterator initialization for all terms in the query
        for (LexiconEntry queryTerm : queryTerms) {
            postingListIterators.add(new PostingListIterator(queryTerm, documentIndex));
        }

        initTime = System.currentTimeMillis();
        return retrieveTopDocuments(postingListIterators, scoringFunction, null, false);
    }

    /**
     * Method to preprocess the query and prepare all the data structure in order execute
     * the query in a disjunctive mode, with the MaxScore algorithm.
     * @param query
     * @param scoringFunction
     * @return
     * @throws IOException
     */
    public static List<Pair<DocumentDescriptor, Double>> searchWithMaxScore(String query, int scoringFunction) throws IOException {
        documentIndex.prepareToAnswerQuery();
        List<PostingListIterator> postingListIterators = new ArrayList<>();

        // Query preprocessing
        List<LexiconEntry> queryTerms = queryPreprocessing(query, false);
        if (queryTerms == null) {
            lastQueryTime = 0;
            return null;
        }

        // Sorting of query terms by upper bound considering the selected scoring function
        if (scoringFunction == Configuration.TFIDF) {
            Collections.sort(queryTerms, QueryProcessor::compareTFIDFUpperBounds);
        }
        if (scoringFunction == Configuration.BM25) {
            Collections.sort(queryTerms, QueryProcessor::compareBM25UpperBounds);
        }

        // Posting list iterator initialization for all terms in the query
        for (int i = 0; i < queryTerms.size(); i++) {
            postingListIterators.add(new PostingListIterator(queryTerms.get(i), documentIndex));
        }

        initTime = System.currentTimeMillis();
        return retrieveTopDocuments(postingListIterators, scoringFunction, queryTerms, false);
    }

    /**
     * Method to preprocess the query and prepare all the data structure in order execute
     * the query in a conjunctive mode, with the BinaryMerge algorithm.
     * @param query
     * @param scoringFunction
     * @return
     * @throws IOException
     */
    public static List<Pair<DocumentDescriptor, Double>> searchConjunctiveDAAT(String query, int scoringFunction) throws IOException {
        documentIndex.prepareToAnswerQuery();
        List<PostingListIterator> postingListIterators = new ArrayList<>();

        // Query preprocessing
        List<LexiconEntry> queryTerms = queryPreprocessing(query, true);
        if (queryTerms == null) {
            lastQueryTime = 0;
            return null;
        }

        // Sorting of query terms by posting lists length
        queryTerms.sort(QueryProcessor::comparePostingListLength);
        // Posting list iterator initialization for all terms in the query
        for (LexiconEntry queryTerm : queryTerms) {
            postingListIterators.add(new PostingListIterator(queryTerm, documentIndex));
        }


        if (queryTerms.size() == 1) {
            initTime = System.currentTimeMillis();
            return retrieveTopDocuments(postingListIterators, scoringFunction, null, false);
        }

        initTime = System.currentTimeMillis();
        return retrieveTopDocuments(postingListIterators, scoringFunction, null, true);
    }

    /**
     * Method to execute the query given: posting lists, scoring function, terms upper bound (that is not null only
     * for the MaxScore algorithm) and if the query is conjunctive or disjunctive.
     * @param postingListIterators
     * @param scoringFunction
     * @param termsUpperBound
     * @param conjunctive
     * @return
     * @throws IOException
     */
    private static List<Pair<DocumentDescriptor, Double>> retrieveTopDocuments(List<PostingListIterator> postingListIterators,
            int scoringFunction, List<LexiconEntry> termsUpperBound, boolean conjunctive) throws IOException {

        TreeSet<Pair<Integer,Double>> topK;

        if(conjunctive)
            topK = ConjunctiveDAAT.executeQuery(postingListIterators, scoringFunction);
        else {
            if (termsUpperBound == null) {
                topK = DocumentAtATime.executeQuery(postingListIterators, scoringFunction);
            } else {
                topK = MaxScore.executeQuery(postingListIterators, termsUpperBound, scoringFunction);
            }
        }

        long endTime = System.currentTimeMillis();
        lastQueryTime = endTime - initTime;

        // Output data structure composed by entry of the document index that correspond to the considered
        // document in the top K and the score associated to that document (calculated with the given scoring
        // function).
        List<Pair<DocumentDescriptor, Double>> output = new ArrayList<>();
        for (Pair<Integer, Double> queryResult: topK) {
            output.add(Pair.of(documentIndex.getAtIndex(queryResult.getKey()), queryResult.getValue()));
        }

        closePostingListIterators(postingListIterators);
        return output;
    }

    /**
     * Method called after the execution of the query in order to close all the posting list iterators used
     * @param iterators
     * @throws IOException
     */
    private static void closePostingListIterators(List<PostingListIterator> iterators) throws IOException {
        for (PostingListIterator p: iterators) {
            p.closeFiles();
        }
    }

    /**
     * Method to preprocess query received and send the list of lexicon entries for each term of the query
     * @param query
     * @param conjunctive
     * @return
     */
    private static List<LexiconEntry> queryPreprocessing (String query, boolean conjunctive) {
        TextPreprocessing textPreprocessor = TextPreprocessing.getInstance();
        // Query preprocessing in order to obtain all valid, distinct query terms in the query
        List<String> queryTerms = textPreprocessor.preprocessTextDistinct(query);

        List<LexiconEntry> lexiconEntryList = new ArrayList<>();
        for (String queryTerm : queryTerms) {
            // Check the availability of the term in the lexicon and if it is present retrieve the associated
            // lexicon entry
            LexiconEntry lexiconEntry = lexicon.get(queryTerm);

            // If the query term is not present in the lexicon, and we want to perform a disjunctive query
            // we skip that term; otherwise if we want to perform a conjunctive query we return null because
            // one (or more) query terms is not available in the lexicon
            if (lexiconEntry != null) {
                lexiconEntryList.add(lexiconEntry);
            }
            else {
                if(conjunctive)
                    return null;
            }
        }
        return lexiconEntryList.size() == 0 ? null : lexiconEntryList;
    }

    /**
     * Comparator of two terms' TFIDF upper bounds, taken from the corresponded lexicon entry
     * @param a
     * @param b
     * @return
     */
    private static int compareTFIDFUpperBounds(LexiconEntry a, LexiconEntry b) {
        return a.getUpperBoundTFIDF().compareTo(b.getUpperBoundTFIDF());
    }

    /**
     * Comparator of two terms' BM25 upper bounds, taken from corresponded the lexicon entry
     * @param a
     * @param b
     * @return
     */
    private static int compareBM25UpperBounds(LexiconEntry a, LexiconEntry b) {
        return a.getUpperBoundBM25().compareTo(b.getUpperBoundBM25());
    }

    /**
     * Comparator of two terms' posting lists length, taken from corresponded the lexicon entry
     * @param a
     * @param b
     * @return
     */
    private static int comparePostingListLength(LexiconEntry a, LexiconEntry b) {
        return Integer.compare(a.getDocfreq(), b.getDocfreq());
    }

    /**
     * Getter of the response time of the query
     * @return
     */
    public static long getResponseTime() {
        return lastQueryTime;
    }

    /**
     * Method to close lexicon and document index
     */
    public static void closeFiles() {
        lexicon.closeFile();
        documentIndex.closeFile();
    }

    /**
     * Comparator of two pairs composed by docid and its score
     * @param a
     * @param b
     * @return
     */
    public static int compareQueryResults(Pair<Integer, Double> a, Pair<Integer, Double> b) {
        int compareScore = -a.getValue().compareTo(b.getValue());
        if (compareScore != 0) {
            return compareScore;
        } else {
            return Integer.compare(a.getKey(), b.getKey());
        }
    }

    /**
     * Method to search the minimum docid across all the posting lists
     * @param postingListIterators
     * @return
     */
    public static Integer minimumDocid(List<PostingListIterator> postingListIterators) throws IOException {
        int minDocid = Integer.MAX_VALUE;
        for (PostingListIterator postingListIterator : postingListIterators) {
            int currentDocid = postingListIterator.next().getKey();
            if (minDocid > currentDocid)
                minDocid = currentDocid;
        }
        return minDocid;
    }
}
