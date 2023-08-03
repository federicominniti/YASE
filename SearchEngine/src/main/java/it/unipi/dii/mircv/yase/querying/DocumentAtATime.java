package it.unipi.dii.mircv.yase.querying;

import it.unipi.dii.mircv.yase.util.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;

/**
 * Implements DAAT algorithm for query processing
 */
public class DocumentAtATime {
    /**
     * Given a list of posting list iterators of the query terms and a scoring function, execute the query with DAAT
     * @param postingListIterators
     * @param scoringFunction
     * @return
     * @throws IOException
     */
    public static TreeSet<Pair<Integer, Double>> executeQuery(List<PostingListIterator> postingListIterators, int scoringFunction) throws IOException {
        TreeSet<Pair<Integer, Double>> sortedTopDocuments = new TreeSet<>(QueryProcessor::compareQueryResults);
        Integer currentMinDocid = QueryProcessor.minimumDocid(postingListIterators);

        while(currentMinDocid != null){
            double score = 0;
            Integer nextDocid = Integer.MAX_VALUE;

            // scan in parallel all the posting list iterators to score the current minimum docid
            for(PostingListIterator postingListIterator : postingListIterators){

                Integer docid = postingListIterator.getCurrentPosting().getKey();

                if(docid != null && docid.intValue() == currentMinDocid){
                    // The current docid of this posting list iterator is equal to current minimum docid;
                    // the score (considering the given scoring function) is updated and the iterator is moved to the
                    // next posting
                    score += postingListIterator.getScore(scoringFunction);
                    postingListIterator.next();
                }

                // find the next minimum docid
                if(postingListIterator.getCurrentPosting().getKey() != null &&
                        postingListIterator.getCurrentPosting().getKey() < nextDocid){
                    nextDocid = postingListIterator.getCurrentPosting().getKey();
                }
            }

            // add the scored document to the result list, maintaining only the top K documents
            sortedTopDocuments.add(Pair.of(currentMinDocid, score));
            if(sortedTopDocuments.size() > Configuration.topK)
                sortedTopDocuments.remove(sortedTopDocuments.last());

            if(nextDocid == Integer.MAX_VALUE)
                currentMinDocid = null;
            else currentMinDocid = nextDocid;
        }

        return sortedTopDocuments;
    }
}
