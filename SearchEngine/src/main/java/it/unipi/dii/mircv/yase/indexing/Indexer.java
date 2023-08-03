package it.unipi.dii.mircv.yase.indexing;

import it.unipi.dii.mircv.yase.structures.*;
import it.unipi.dii.mircv.yase.util.Configuration;
import it.unipi.dii.mircv.yase.util.Util;
import it.unipi.dii.mircv.yase.util.preprocessing.TextPreprocessing;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class used to manage the indexing phase.
 */
public class Indexer {
    private static AtomicInteger currentDocId;
    private static AtomicInteger currentCollectionLength;
    private static int currentBlock = 0;
    private static List<DocumentDescriptor> chunkDocumentIndex;
    private static DocumentIndex documentIndex;
    private static final TextPreprocessing textPreprocessing = TextPreprocessing.getInstance();
    private final static Logger logger = Logger.getLogger(Indexer.class);


    /**
     * method used to launch the indexing of the collection measuring the time involved.
     */
    public static void createIndex() {

        logger.info("Start indexing.");
        long initTime = System.currentTimeMillis();
        // init fields
        currentDocId = new AtomicInteger(0);
        currentCollectionLength = new AtomicInteger(0);
        chunkDocumentIndex = Collections.synchronizedList(new ArrayList<>());
        // init the document index
        documentIndex = new DocumentIndex(Configuration.finalDir,
                Configuration.documentIndexFinalName, Configuration.docnoLength);
        try {
            documentIndex.prepareForBatchWrites();
        } catch (IOException e) {
            logger.fatal(e.getMessage());
            System.exit(-1);
        }

        // start processing the collection
        processCollection();
        // merge all the partial data structures
        mergePartialInvertedIndexes();
        long endTime = System.currentTimeMillis();
        logger.info("End indexing (" + (endTime - initTime)/60000 + " min).");
    }

    /**
     * Start building the index using the SPIMI algorithm.
     */
    private static void processCollection(){
        logger.info("Start processing the collection.");
        // take the collection
        TarArchiveInputStream tarInput = null;
        try {
            tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(Files.newInputStream(Paths.get(Configuration.indexingCollection))));
            tarInput.getNextTarEntry();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        if (tarInput == null) {
            logger.fatal("Cannot access to the collection.");
            System.exit(-1);
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(tarInput));

        // List of documents to process together
        List<String> chunkOfDocuments = new ArrayList<>();
        HashMap<String, PostingList> partialIndex = new HashMap<>();
        String document;
        try {
            // read a document of the collection
            document = bufferedReader.readLine();
            while (true) {
                chunkOfDocuments.add(document);
                document = bufferedReader.readLine();
                // check if we have added enough docs in the chunk or if we have process all the collection
                if(chunkOfDocuments.size() % Configuration.indexingChunkSize == 0 || document == null) {
                    // build the partial inverted index on the portion of collection collected
                    HashMap<String, PostingList> processedChunk = processChunk(chunkOfDocuments);

                    if (partialIndex.size() == 0)
                        // case of first partial inverted index
                        partialIndex = processedChunk;
                    else
                        // merge the partial inverted index with the existing one
                        mergeProcessedChunks(partialIndex, processedChunk);

                    // check if the usage of memory is over a certain threshold or at the end of the merging phase
                    if (Util.getFreeHeapPercentage() < Configuration.indexingMinHeap || document == null) {
                        // write partial data structure to disk
                        writeToDiskPartialDocIndex();
                        writeBlockToDisk(partialIndex);
                        logger.info("write the block n." + currentBlock);
                        // clean the data structure
                        chunkDocumentIndex.clear();
                        partialIndex.clear();
                        // force the Garbage Collector
                        while (Util.getFreeHeapPercentage() < 1-Configuration.indexingMinHeap)
                            System.gc();

                        currentBlock++;
                        // check if the merging phase is over
                        if (document == null)
                            break;
                    }
                    // clear the portion of docs collected
                    chunkOfDocuments.clear();
                }
            }
            // write on file the collections statistics
            CollectionStatistics collectionStatistics = CollectionStatistics.getInstance();
            collectionStatistics.setNumberOfDocuments(currentDocId.get());
            collectionStatistics.setCollectionDocumentLength(currentCollectionLength.get());
            logger.info("Update collection statistics.");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Method used in order to merge all the partial block and build the final inverted index and the
     * final {@link Lexicon}.
     */
    private static void mergePartialInvertedIndexes() {
        // utility data structure
        List<PartialLexicon> partialLexicons = new ArrayList<>();
        List<Integer> currentIndexes = new ArrayList<>();
        List<Integer> partialLexiconsSizes = new ArrayList<>();

        List<LexiconEntry> finalLexiconEntries = new ArrayList<>();

        // prepare the files for the final inverted index
        IndexingIO.prepareForBatchWrites(
                Configuration.finalDir + Configuration.invertedIndexTFsName,
                Configuration.finalDir + Configuration.invertedIndexDocidsName
        );

        // open the final document index, will be useful to compute the upper bounds
        try {
            documentIndex.closeWriter();
            documentIndex.openFile(true);
            documentIndex.enableMemoryMap();
        } catch (IOException e) {
            logger.fatal(e.getMessage());
            System.exit(-1);
        }

        // priority queue used to keep lexicon entries sorted by term
        PriorityQueue<Pair<LexiconEntry, Integer>> priorityQueue = new PriorityQueue<>(Indexer::comparePairsDuringMerge);

        // iterate over each block previously created
        for (int index = 0; index < currentBlock; index++) {
            // put all the partial lexicons in a list
            partialLexicons.add(
                    new PartialLexicon(
                            Configuration.lexiconPartialDir,
                            Configuration.lexiconPartialPrefix + index,
                            Configuration.termLength
                    )
            );
            partialLexiconsSizes.add(partialLexicons.get(index).getNumEntries());
            currentIndexes.add(0);
            partialLexicons.get(index).openFile(true);
            partialLexicons.get(index).enableMemoryMap();
            // save the first entry of each partial lexicon
            priorityQueue.add(Pair.of(partialLexicons.get(index).readNextMMap(), index));
        }

        // check for errors
        if (priorityQueue.size() == 0) {
            logger.fatal("Not partial data structures found.");
            System.exit(1);
        }

        // until we don't process each term of the final vocabulary
        while (priorityQueue.size() > 0) {
            // take and remove the first lexicon entry
            Pair<LexiconEntry, Integer> peek = priorityQueue.poll();

            // that's why we saved also the block id
            LexiconEntry currentLexiconEntry = peek.getKey();
            int blockId = peek.getValue();

            // check if we have processed all the block
            if (currentIndexes.get(blockId) < partialLexiconsSizes.get(blockId) - 1) {
                // save the number of processed entries for each block
                int counter = currentIndexes.get(blockId);
                currentIndexes.set(blockId, counter+1);
                priorityQueue.add(Pair.of(partialLexicons.get(blockId).readNextMMap(), blockId));
            }

            // read the posting list relative to the specific block and lexicon entry
            PostingList aggregatedPL = IndexingIO.readFromPartialIndexFile(currentLexiconEntry.getOffsetDocid(), blockId);

            // we will check if the next entry has the same term
            Pair<LexiconEntry, Integer> duplicatePeek = priorityQueue.peek();
            LexiconEntry duplicateLexiconEntry = null;
            if (duplicatePeek != null)
                duplicateLexiconEntry = duplicatePeek.getKey();

            // until the next entry in the priority queue has the same term of the previous
            // we merge their posting list.
            while (priorityQueue.size() != 0 &&
                    duplicateLexiconEntry != null &&
                    duplicateLexiconEntry.getTerm().equals(currentLexiconEntry.getTerm())
            ) {
                int duplicateBlockId = duplicatePeek.getValue();
                // merge the posting list
                PostingList tmp = IndexingIO.readFromPartialIndexFile(duplicatePeek.getKey().getOffsetDocid(), duplicateBlockId);
                aggregatedPL.mergePostings(tmp);
                // remove processed entry
                priorityQueue.poll();
                // check if we have processed all the block
                if (currentIndexes.get(duplicateBlockId) < partialLexiconsSizes.get(duplicateBlockId) - 1) {
                    int counter = currentIndexes.get(duplicateBlockId);
                    currentIndexes.set(duplicateBlockId, counter+1);
                    priorityQueue.add(Pair.of(partialLexicons.get(duplicateBlockId).readNextMMap(), duplicateBlockId));
                }

                // take the next entry in order to check if it corresponds to the same term
                duplicatePeek = priorityQueue.peek();
                if (duplicatePeek == null)
                    duplicateLexiconEntry = null;
                else
                    duplicateLexiconEntry = duplicatePeek.getKey();
            }

            try {
                // write the final posting list relative to this term and take the offsets in order to save it
                // in the lexicon
                Pair<Long, Long> offsets = IndexingIO.writePostingList(aggregatedPL);
                // compute the upper bounds of the posting list
                Pair<Double, Double> weights = computeWeights(aggregatedPL);
                // update the final lexicon
                finalLexiconEntries.add(
                        new LexiconEntry(
                                peek.getKey().getTerm(),
                                aggregatedPL.getPostingsCounter(),
                                offsets.getKey(),
                                offsets.getValue(),
                                weights.getKey(),
                                weights.getValue())
                );
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // write the lexicon on disk
        Lexicon finalLexicon = new Lexicon(
                Configuration.finalDir,
                Configuration.lexiconFinalName,
                Configuration.termLength,
                finalLexiconEntries.size(),
                Configuration.lexiconCache
        );

        finalLexicon.putAllMMap(finalLexiconEntries);
        finalLexicon.closeFile();
        IndexingIO.closeFiles();
        logger.info("Create lexicon.");

        // close all the opened files
        for (int i = 0; i < currentBlock; i++) {
            partialLexicons.get(i).closeFile();
        }

        //Delete all partials created to build the lexicon and inverted index
        partialLexicons.get(0).deleteDirectory();
        try {
            FileUtils.cleanDirectory(new File(Configuration.invertedIndexPartialDir));
            FileUtils.deleteDirectory(new File(Configuration.invertedIndexPartialDir));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        // update the global statistics of the collection
        CollectionStatistics collectionStatistics = CollectionStatistics.getInstance();
        collectionStatistics.setLexiconSize(finalLexiconEntries.size());
        logger.info("Update collection statistics");
    }

    /**
     * Method used to compute the upper bound of a posting list.
     * @param postingList {@link PostingList} to process
     * @return couple with the upper bounds of TF-IDF and BM25
     */
    private static Pair<Double, Double> computeWeights(PostingList postingList) {
        // take global statistics to compute the weights
        CollectionStatistics collectionStatistics = CollectionStatistics.getInstance();
        double avgDocLen = collectionStatistics.getAverageDocumentLength();
        // get postings
        int [] docids = postingList.getDocIds();
        int [] tfs = postingList.getTfs();

        // coefficients to compute BM25
        double b = Configuration.bm25B;
        double k1 = Configuration.bm25K1;

        // init the upper bounds
        double maxTFIDF = 0;
        double maxBM25 = 0;

        // iterate over postings
        for (int i = 0; i < tfs.length; i++) {
            // TF-IDF
            double idf = Math.log((collectionStatistics.getNumberOfDocuments() - postingList.getPostingsCounter() + 0.5d)
                    / (postingList.getPostingsCounter() + 0.5d))
                    / Math.log(2);
            double tf = 1+Math.log(tfs[i])/Math.log(2);
            // update upper bound
            if (maxTFIDF < tf*idf)
                maxTFIDF = tf*idf;

            // BM25
            int dl = documentIndex.getAtIndexMMap(docids[i]).getDocLen();
            double K = k1 * ((1 - b) + b * dl / avgDocLen);
            double bm25 = idf * ((k1 + 1d) * tfs[i] / (K + tfs[i]));
            // update upper bound
            if (maxBM25 < bm25)
                maxBM25 = bm25;
        }

        return Pair.of(maxTFIDF, maxBM25);
    }

    /**
     * Method used to process portion of the collection in order to build partial inverted index.
     *
     * @param chunkOfDocuments portion of the collection
     * @return inverted index of the portion of collection
     */
    private static HashMap<String, PostingList> processChunk(List<String> chunkOfDocuments) {
        // init the inverted index
        HashMap<String, PostingList> block = new HashMap<>();

        // create parallel, unordered stream of lines
        LinkedHashMap<Pair<Integer, String>, Integer> intermediate = chunkOfDocuments.parallelStream()
                // tokenize and get a stream of tuples with the following format: (docid, term)
                .map(Indexer::updatePartialIndex)
                // get stream of tuples of (docid, term)
                .flatMap(Collection::stream)
                // sort the couples in ascending order by term
                .sorted(Indexer::compareByAscedingTermAndDocid)
                //group by (docid, term) and count the occurrences to get the TF
                .collect(Collectors.groupingBy(Function.identity(), LinkedHashMap::new, Collectors.summingInt(x -> 1)));

        // build the partial index with the processed chuck
        intermediate.forEach((key, value) -> buildChunkPostingList(block, key, value));
        return block;
    }

    /**
     *  Method used to process the document, incrementally build the {@link DocumentIndex}
     *  and compute the {@link CollectionStatistics}.
     *
     * @param document of the collection
     * @return list of couples <docid, word>
     */
    private static List<Pair<Integer, String>> updatePartialIndex(String document){
        //return the docNo and the respective list of tokens of the doc
        Pair<String, List<String>> docProcessed = textPreprocessing.tokenize(document);
        String docNo = docProcessed.getKey();
        List<String> tokens = docProcessed.getValue();
        int docid = currentDocId.getAndIncrement();
        // build the document index, docid -> <docNo, doc length>
        chunkDocumentIndex.add(new DocumentDescriptor(docid, docNo, tokens.size()));
        // update the global statistic
        currentCollectionLength.addAndGet(tokens.size());
        // attach to each token the respective docid
        return tokens.stream()
                .map(word -> Pair.of(docid, word))
                .collect(Collectors.toList());
    }

    /**
     * Method used to sort the couples <docid, term> in ascending order by term.
     * @param a couple <docid, term>
     * @param b couple <docid, term>
     * @return boolean order
     */
    private static int compareByAscedingTermAndDocid(Pair<Integer, String> a, Pair<Integer, String> b) {
        int compareTerms = a.getValue().compareTo(b.getValue());
        // compare first by term
        if (compareTerms != 0) {
            return compareTerms;
        } else {
            // if terms are equal we compare by docid
            return Integer.compare(a.getKey(), b.getKey());
        }
    }

    /**
     * Method used to sort the Lexicon Entry by term
     * @param a couple <{@link LexiconEntry}, blockId>
     * @param b couple <{@link LexiconEntry}, blockId>
     * @return represents the order
     */
    private static int comparePairsDuringMerge(Pair<LexiconEntry, Integer> a, Pair<LexiconEntry, Integer> b) {
        int compareTerms = a.getKey().compareTo(b.getKey());
        if (compareTerms != 0) {
            return compareTerms;
        } else {
            return Integer.compare(a.getValue(), b.getValue());
        }
    }

    /**
     * Method used to build the inverted index from the portion of collections already processed.
     * @param processedChunk partial inverted index
     * @param docidTerm couple <docid, term>
     * @param TF term frequency
     */
    private static void buildChunkPostingList(HashMap<String, PostingList> processedChunk, Pair<Integer, String> docidTerm, Integer TF){
        int docid = docidTerm.getKey();
        String term = docidTerm.getValue();
        PostingList postingList;
        // check if the posting list already exist
        if ((postingList = processedChunk.get(term)) == null) {
            // init the posting list with the first posting
            PostingList postingListTemp = new PostingList(new int[]{docid}, new int[]{TF});
            processedChunk.put(term, postingListTemp);
        } else {
            // posting list already exist, so we just add the posting
            postingList.addEntry(docid, TF);
            processedChunk.put(term, postingList);
        }
    }

    /**
     * Method used to merge two inverted index.
     * @param a inverted index
     * @param b inverted index
     */
    private static void mergeProcessedChunks(HashMap<String, PostingList> a, HashMap<String, PostingList> b) {
        // iterate over each entry of one inverted index
        for (String term: b.keySet()) {
            PostingList postingListTemp;
            if ((postingListTemp = a.get(term)) != null) {
                // both the inverted index have the entry for this term
                postingListTemp.mergePostings(b.get(term));
            } else {
                // only one inverted index has the entry for this term
                a.put(term, b.get(term));
            }
        }
    }

    /**
     * Method used to write on disk the {@link DocumentIndex} information.
     * @throws IOException if there are problem while writing on file
     */
    private static void writeToDiskPartialDocIndex() throws IOException {
        // process each entry as a stream in order to retrieve a sorted list of document descriptor
        List<DocumentDescriptor> orderedDocIndex = chunkDocumentIndex.parallelStream()
                .sorted(DocumentDescriptor::compareTo)
                .collect(Collectors.toList());

        orderedDocIndex.forEach(documentDescriptor -> {
            try {
                documentIndex.append(documentDescriptor);
            } catch (IOException e) {
                    logger.fatal(e.getMessage());
            }
        });
    }

    /**
     * method used to write the partial inverted index and the partial lexicon on file
     * @param block partial inverted list
     * @throws IOException if there are problem while writing on the files.
     */
    private static void writeBlockToDisk(HashMap<String, PostingList> block) throws IOException {
        // write on file the inverted index and build the relative vocabulary
        List<LexiconEntry> partialVocabulary = IndexingIO.writePartialIndexFromBlock(block, currentBlock);
        // init the partial lexicon file
        PartialLexicon partialLexicon = new PartialLexicon(
                Configuration.lexiconPartialDir,
                Configuration.lexiconPartialPrefix + currentBlock,
                Configuration.termLength
        );
        // write the partial lexicon on file
        partialLexicon.putAllMMap(partialVocabulary);
    }

}
