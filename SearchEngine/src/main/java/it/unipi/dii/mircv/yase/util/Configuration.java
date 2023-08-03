package it.unipi.dii.mircv.yase.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Singleton class in order to maintain all the configuration variables of the project
 */
public class Configuration {
    // Properties file from which we retrieve configuration parameters
    private final Properties prop;

    // Path of the properties file
    private static final String path = "SearchEngine/src/main/resources/config.properties";

    // Scoring function values
    public static final int TFIDF = 1;
    public static final int BM25 = 2;

    public static final int CONJUNCTIVE = 1;
    public static final int DISJUNCTIVE = 2;
    public static final int DISJUNCTIVE_OPT = 3;

    // Document index configuration variables:

    // Maximum docno length
    public static int docnoLength;
    // Document index paths
    public static String documentIndexPartialDir;
    public static String documentIndexPartialPrefix;
    public static String documentIndexFinalName;
    // Document index buffer size
    public static int documentIndexLocalBufferSize;

    // Lexicon configuration variables:

    // Maximum term length
    public static int termLength;
    // Lexicon paths
    public static String lexiconPartialDir;
    public static String lexiconPartialPrefix;
    public static String lexiconFinalName;
    // Stopwords file name
    public static String stopwordsFile;
    // Lexicon cache size
    public static int lexiconCache;
    // Flag to remove stopwords
    public static boolean useStopwords;
    // Flag to stem words
    public static boolean useStemming;

    // Inverted index configuration variables:

    // Inverted index paths
    public static String invertedIndexPartialDir;
    public static String invertedIndexPartialPrefix;
    public static String invertedIndexTFsName;
    public static String invertedIndexDocidsName;
    // Limit beyond which start to use skip blocks
    public static int indexingSkipBlocksThreshold;

    // Indexing configuration variables:

    // Collection path
    public static String indexingCollection;
    // Number of documents for each chunk
    public static int indexingChunkSize;
    // Minimum percentage of free heap to maintain during indexing
    public static double indexingMinHeap;

    // Directory containing the index files
    public static String finalDir;

    // Scoring parameters
    public static double bm25K1;
    public static double bm25B;

    // Number of results to retrieve in a query
    public static int topK;

    // Number of warmup queries
    public static int warmUp;
    //queries for evaluation
    public static String evalQueries;

    static {
        new Configuration();
    }

    private Configuration(){
        prop = new Properties();
        try {
            prop.load(Files.newInputStream(Paths.get(path)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        docnoLength = Integer.parseInt(prop.getProperty("document_index.docno_length"));
        documentIndexPartialDir = prop.getProperty("document_index.partial_dir");
        documentIndexPartialPrefix = prop.getProperty("document_index.partial_prefix");
        documentIndexFinalName = prop.getProperty("document_index.final_name");
        documentIndexLocalBufferSize = Integer.parseInt(prop.getProperty("document_index.local_buffer_size"));

        termLength = Integer.parseInt(prop.getProperty("lexicon.term_length"));
        lexiconPartialDir = prop.getProperty("lexicon.partial_dir");
        lexiconPartialPrefix = prop.getProperty("lexicon.partial_prefix");
        stopwordsFile = prop.getProperty("lexicon.stopwords_file");
        lexiconFinalName = prop.getProperty("lexicon.final_name");
        lexiconCache = Integer.parseInt(prop.getProperty("lexicon.cache"));
        useStopwords = Boolean.parseBoolean(prop.getProperty("lexicon.use_stopwords"));
        useStemming = Boolean.parseBoolean(prop.getProperty("lexicon.stemming"));

        invertedIndexPartialPrefix = prop.getProperty("inverted_index.partial_prefix");
        invertedIndexPartialDir = prop.getProperty("inverted_index.partial_dir");
        invertedIndexTFsName = prop.getProperty("inverted_index.tfs_name");
        invertedIndexDocidsName = prop.getProperty("inverted_index.docids_name");
        indexingSkipBlocksThreshold = Integer.parseInt(prop.getProperty("inverted_index.skip_blocks_threshold"));

        indexingCollection = prop.getProperty("indexing.collection_file");
        indexingChunkSize = Integer.parseInt(prop.getProperty("indexing.chunk_size"));
        indexingMinHeap = Double.parseDouble(prop.getProperty("indexing.min_available_heap"));

        finalDir = prop.getProperty("yase.structures");

        bm25K1 = Double.parseDouble(prop.getProperty("scoring.bm25.k1"));
        bm25B = Double.parseDouble(prop.getProperty("scoring.bm25.b"));

        topK = Integer.parseInt(prop.getProperty("daat.topK"));
        warmUp = Integer.parseInt(prop.getProperty("search.warmup"));
        evalQueries = prop.getProperty("evaluation.queries");
    }
}
