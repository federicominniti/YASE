package it.unipi.dii.mircv.yase.querying;

import it.unipi.dii.mircv.yase.structures.LexiconEntry;
import it.unipi.dii.mircv.yase.util.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;


/**
 * Class to handle query processing with MaxScore
 */
public class MaxScore {
    /**
     * Given a list of posting list iterators of the query terms sorted by increasing term upper bound,
     * the list of term upper bounds and a scoring function, execute the query with MaxScore
     * @param postingListIterators
     * @param termsUpperBound
     * @param scoringFunction
     * @return
     */
    public static TreeSet<Pair<Integer, Double>> executeQuery(List<PostingListIterator> postingListIterators, List<LexiconEntry> termsUpperBound, int scoringFunction) throws IOException {
        TreeSet<Pair<Integer, Double>> sortedTopDocuments = new TreeSet<>(QueryProcessor::compareQueryResults);
        double[] upperBounds = new double[termsUpperBound.size()];

        // Initialization of upper bounds' limits
        upperBounds[0] = termsUpperBound.get(0).getUpperBound(scoringFunction);
        for (int i = 1; i < termsUpperBound.size(); i++) {
            upperBounds[i] = upperBounds[i-1] + termsUpperBound.get(i).getUpperBound(scoringFunction);
        }

        // Threshold to overcome in order to enter in top K
        double threshold = 0;

        // Pivot index to divide non-essential posting lists and essential posting list, in the list of posting list
        // iterators
        int pivot = 0;
        Integer currentMinDocid = QueryProcessor.minimumDocid(postingListIterators);

        while (pivot < upperBounds.length && currentMinDocid != null) {
            double score = 0;
            int next = Integer.MAX_VALUE;

            // Check first the essential posting lists in order to calculate a partial score
            // At the same time we search the next docid to process
            for (int i = pivot; i < upperBounds.length; i++) {
                PostingListIterator postingList = postingListIterators.get(i);
                if (postingList.getCurrentPosting().getKey() != null &&
                        postingList.getCurrentPosting().getKey().equals(currentMinDocid)) {
                    score += postingList.getScore(scoringFunction);
                    postingList.next();
                }
                if (postingList.getCurrentPosting().getKey() != null &&
                        postingList.getCurrentPosting().getKey() < next) {
                    next = postingList.getCurrentPosting().getKey();
                }
            }

            // Check non-essential posting lists in order to calculate the full score
            for (int i = pivot-1; i >= 0; i--) {
                PostingListIterator postingList = postingListIterators.get(i);
                if (score + upperBounds[i] <= threshold) {
                    // if the current score plus the sum of remaining terms upper bounds doesn't overcome the threshold
                    // we can early stop this document's scoring process
                    break;
                }

                // move the posting list iterator to the current minimum docid (if present)
                postingList.nextGeq(currentMinDocid);
                if (postingList.getCurrentPosting().getKey() != null &&
                        postingList.getCurrentPosting().getKey().equals(currentMinDocid)) {
                    // the current docid is actually present, so we can update the score
                    score += postingList.getScore(scoringFunction);
                }
            }

            // Result list update
            Pair<Integer, Double> candidate = Pair.of(currentMinDocid, score);
            if (sortedTopDocuments.size() < Configuration.topK || QueryProcessor.compareQueryResults(candidate, sortedTopDocuments.last()) < 0) {

                // current min docid enters the top K
                sortedTopDocuments.add(candidate);
                if (sortedTopDocuments.size() > Configuration.topK)
                    sortedTopDocuments.remove(sortedTopDocuments.last());

                if (Configuration.topK == sortedTopDocuments.size()) {
                    // We already have K elements
                    // We update threshold with the score of the last document in the top-K (smallest)
                    threshold = sortedTopDocuments.last().getValue();

                    // We move the pivot to the first posting list that has an upper bound's limit that is greater than
                    // the threshold
                    while (pivot < upperBounds.length && upperBounds[pivot] <= threshold) {
                        pivot++;
                    }
                }
            }

            // Update of the current min docid
            if(next == Integer.MAX_VALUE)
                currentMinDocid = null;
            else
                currentMinDocid = next;

        }

        return sortedTopDocuments;
    }
}
