package it.unipi.dii.mircv.yase;
import it.unipi.dii.mircv.yase.indexing.Indexer;
import it.unipi.dii.mircv.yase.querying.QueryProcessor;
import it.unipi.dii.mircv.yase.structures.DocumentDescriptor;
import it.unipi.dii.mircv.yase.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.cli.*;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

public class Yase {

    static {
        initLogger();
    }


    private static final Option ARG_TEST = new Option(
            "t",
            "test",
            false,
            "Creates a results file ready to be used to evaluate the effectiveness\n" +
            "of the search engine with trec_eval (Index is needed)."
    );
    private static final Option ARG_INDEX = new Option(
            "i",
            "index",
            false,
            "Create the index by processing the collection at the following path: " +
                    "'SearchEngine/src/main/resources/collection.tar.gz' (MSMARCO collection)"
    );
    private static final Option ARG_SEARCH = new Option(
            "s",
            "search",
            false,
            "Start using the search engine (Index is needed)."
    );

    private static int scoringFunction = -1;
    private static int queryMode = -1;
    private static int topK = -1;
    private final static Logger logger = Logger.getLogger(Yase.class);

    public static void main(String[] args) throws IOException {
        CommandLineParser clp = new DefaultParser();
        Options options = new Options();
        options.addOption(ARG_INDEX);
        options.addOption(ARG_TEST);
        options.addOption(ARG_SEARCH);

        try {
            CommandLine cl = clp.parse(options, args);
            if (cl.hasOption(ARG_INDEX)) {
                File collection = new File(Configuration.indexingCollection);
                if (!collection.exists()) {
                    printHelp(options);
                    System.exit(-1);
                }

                cleanEnvironment();
                Indexer.createIndex();
            } else if (cl.hasOption(ARG_TEST)) {
                if (!existsIndex()) {
                    printHelp(options);
                    System.exit(-1);
                }

                testQueries();
            } else if (cl.hasOption(ARG_SEARCH)) {
                if (!existsIndex()) {
                    printHelp(options);
                    System.exit(-1);
                }
                logger.info("Starting warm-up");
                warmUp();
                logger.info("Warm-up OK");
                String name =   " ______  \n" +
                                "|      | \n" +
                                "| YASE | \n" +
                                "|______| ";
                System.out.println(name);
                System.out.println();
                System.out.println();
                startCLI();
            } else {
                printHelp(options);
            }
        } catch (Exception e) {
            logger.error("Error during parsing of the arguments.", e);
        }

    }


    public static void startCLI() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            if (scoringFunction < 0) {
                System.out.println("Select scoring function: [tf-idf, bm25]");
                System.out.print("> ");
                String score = reader.readLine();
                switch (score) {
                    case "tf-idf":
                        scoringFunction = Configuration.TFIDF;
                        break;
                    case "bm25":
                        scoringFunction = Configuration.BM25;
                        break;
                    default:
                        System.out.println("Wrong argument.");
                        continue;
                }
            }
            if (queryMode < 0) {
                System.out.println("Select type of queries: [and, or, or+]");
                System.out.print("> ");
                String mode = reader.readLine();
                switch (mode) {
                    case "and":
                        queryMode = Configuration.CONJUNCTIVE;
                        break;
                    case "or":
                        queryMode = Configuration.DISJUNCTIVE;
                        break;
                    case "or+":
                        queryMode = Configuration.DISJUNCTIVE_OPT;
                        break;
                    default:
                        System.out.println("Wrong argument.");
                        continue;
                }
            }
            if (topK < 0) {
                System.out.println("Select number of results to print (NB: K=" + Configuration.topK + "): ");
                System.out.print("> ");
                int k;
                try {
                    k = Integer.parseInt(reader.readLine());
                } catch (Exception ex) {
                    System.out.println("Wrong argument.");
                    continue;
                }
                if (k <= 0 || k > Configuration.topK) {
                    System.out.println("Not valid number.");
                } else {
                    topK = k;
                }
            }
            if (scoringFunction < 0 || queryMode < 0 || topK < 0)
                continue;

            System.out.println("Write your query: [_exit to close and _reset to change settings]");
            System.out.print("> ");
            String query = reader.readLine();
            switch (query) {
                case "_exit":
                    logger.info("Close YASE.");
                    QueryProcessor.closeFiles();
                    System.exit(0);
                case "_reset":
                    queryMode = -1;
                    scoringFunction = -1;
                    topK = -1;
                    break;
                default:
                    if (queryMode == Configuration.CONJUNCTIVE) {
                        printQueryResults(QueryProcessor.searchConjunctiveDAAT(query, scoringFunction));
                    } else if (queryMode == Configuration.DISJUNCTIVE) {
                        printQueryResults(QueryProcessor.searchWithDAAT(query, scoringFunction));
                    } else if (queryMode == Configuration.DISJUNCTIVE_OPT) {
                        printQueryResults(QueryProcessor.searchWithMaxScore(query, scoringFunction));
                    }
                    System.out.println(QueryProcessor.getResponseTime() + " ms " + "(K="+Configuration.topK+")");
            }
        }
    }

    public static void printQueryResults(List<Pair<DocumentDescriptor, Double>> results) {
        if (results == null) {
            System.out.println("No results found.");
            return;
        }

        System.out.println("Showing " + topK + " results out of " + Configuration.topK);
        System.out.println("rank\tdocno\tscore");
        for (int i = 0; i < topK && i < results.size(); i++) {
            Pair<DocumentDescriptor, Double> result = results.get(i);
            System.out.println((i+1) + "\t" + result.getKey().getDocNo() + "\t" + String.format("%6f", result.getValue()));
        }
    }

    /**
     * A brief warm-up at the start of the search engine in search mode
     */
    public static void warmUp() throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(Configuration.evalQueries));
        String line;
        int count = 0;
        long warmUpTime = 0;
        while ((line = bf.readLine()) != null && count < Configuration.warmUp) {
            String query = line.split("\t")[1];
            QueryProcessor.searchWithMaxScore(query, Configuration.BM25);
            warmUpTime += QueryProcessor.getResponseTime();
            count++;
        }

        logger.info(count + " queries, total ms: " + warmUpTime);
    }

    /**
     * Creates a results file ready to be used to evaluate the effectiveness
     * of the search engine with trec_eval
     */
    public static void testQueries() throws IOException {
        logger.info("Testing Queries -- writing results to 'results.txt'");
        String outputName = "results.txt";
        File res = new File(outputName);
        if (res.exists())
            res.delete();

        BufferedReader bf = new BufferedReader(new FileReader(Configuration.evalQueries));
        String line;
        int count = 0;

        BufferedWriter resultsBF;
        resultsBF = new BufferedWriter(new FileWriter(outputName));

        long totalProcessingTime = 0;
        while ((line = bf.readLine()) != null) {
            String[] evaluationQuery = line.split("\t");
            String queryId = evaluationQuery[0];
            String query = evaluationQuery[1];
            List<Pair<DocumentDescriptor, Double>> results = QueryProcessor.searchWithMaxScore(query, Configuration.BM25);
                for(int i = 0; results!=null && i<results.size(); i++){
                    String queryResult = queryId + " Q0 " + results.get(i).getKey().getDocNo()
                            + " " + (i+1) + " " + results.get(i).getValue() + " YASE\n";
                    resultsBF.write(queryResult);
                }

            totalProcessingTime += QueryProcessor.getResponseTime();
            count++;
        }

        resultsBF.close();

        logger.info("Test completed, mrt: " + totalProcessingTime / count);
    }

    private static void initLogger() {
        PropertyConfigurator.configure("SearchEngine/src/main/resources/log4j.properties");
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);
        pw.println("YASE " + Yase.class.getPackage().getImplementationVersion());
        pw.println();
        formatter.printUsage(pw, 100, "java -jar yase.jar [options]");
        formatter.printOptions(pw, 100, options, 2, 5);
        pw.close();
    }

    private static boolean existsIndex () {
        File lexicon = new File(Configuration.finalDir + Configuration.lexiconFinalName);
        File documentIndex = new File(Configuration.finalDir + Configuration.documentIndexFinalName);
        File invertedIndexTF = new File(Configuration.finalDir + Configuration.invertedIndexTFsName);
        File invertedIndexDocid = new File(Configuration.finalDir + Configuration.invertedIndexDocidsName);

        return lexicon.exists() && documentIndex.exists() && invertedIndexTF.exists() && invertedIndexDocid.exists();
    }

    private static void cleanEnvironment() {
        try {
            FileUtils.deleteDirectory(new File(Configuration.finalDir));
            FileUtils.deleteDirectory(new File(Configuration.invertedIndexPartialDir));
            FileUtils.deleteDirectory(new File(Configuration.documentIndexPartialDir));
            FileUtils.deleteDirectory(new File(Configuration.lexiconPartialDir));
        } catch (IOException e) {
            logger.error("Error while cleaning index directories");
        }
    }
}
