package it.unipi.dii.mircv.yase.querying;

import it.unipi.dii.mircv.yase.util.Configuration;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;

/**
 * Class to process conjunctive queries
 */
public class ConjunctiveDAAT {
    /**
     * Given a list of posting list iterators of the query terms and a scoring function, execute the query in a
     * DAAT + conjunctive mode. The iterators are already sorted in increasing posting list's length
     * @param postingListIterators
     * @param scoringFunction
     * @throws IOException
     */
    public static TreeSet<Pair<Integer, Double>> executeQuery(List<PostingListIterator> postingListIterators, int scoringFunction) {
        HashMap<Integer, Double> mergedPostingLists = new HashMap<>();
        postingListIterators.get(0).next();
        Integer currentDocid = postingListIterators.get(0).getCurrentPosting().getKey();

        while (currentDocid != null) {
            boolean docIdNotPresent = false;

            for (int i = 1; i < postingListIterators.size(); i++) {
                // search in each posting list the current docid taken from the smallest posting list
                postingListIterators.get(i).nextGeq(currentDocid);
                Integer currentPostingDocId = postingListIterators.get(i).getCurrentPosting().getKey();
                if (currentPostingDocId == null
                        || !currentPostingDocId.equals(currentDocid)) {
                    // the current docid is not present in at least one posting list; in this case we don't have to consider
                    // this document further
                    docIdNotPresent = true;
                    break;
                }

                // The current docid is present in the posting list
                // If the docid is not present in the partial results we add it, and we initialize its score with the score
                // of the document with respect to the smallest posting list.
                // If already present, we just need to update the partial score.
                Double currentValue = mergedPostingLists.get(currentPostingDocId);
                if (currentValue == null) {
                    currentValue = postingListIterators.get(0).getScore(scoringFunction);
                }
                currentValue += postingListIterators.get(i).getScore(scoringFunction);
                mergedPostingLists.put(currentPostingDocId, currentValue);
            }

            // there is a posting list that doesn't have the current docid, hence we have to remove it from the results
            if (docIdNotPresent)
                mergedPostingLists.remove(currentDocid);

            // go to the next posting in the smallest posting list
            postingListIterators.get(0).next();
            currentDocid = postingListIterators.get(0).getCurrentPosting().getKey();
        }

        // sort the documents in descending order of score, and take only the top k
        TreeSet<Pair<Integer, Double>> sortedTopDocuments = new TreeSet<>(QueryProcessor::compareQueryResults);
        for(Integer key: mergedPostingLists.keySet()){
            Pair<Integer, Double> candidate = Pair.of(key, mergedPostingLists.get(key));
            if (sortedTopDocuments.size() < Configuration.topK ||
                    QueryProcessor.compareQueryResults(candidate, sortedTopDocuments.last()) < 0) {

                sortedTopDocuments.add(candidate);
                if (sortedTopDocuments.size() > Configuration.topK)
                    sortedTopDocuments.remove(sortedTopDocuments.last());
            }
        }

        return sortedTopDocuments;
    }
}
